from typing import Callable, Optional, Sequence, Union

import clip
import torch
from PIL import Image
from PIL.PngImagePlugin import PngImageFile
from torch.utils.data import DataLoader, Dataset
from torchvision.transforms import Compose, ToTensor
from tqdm import tqdm


class ImageOnlyDataset(Dataset[torch.Tensor]):
    def __init__(
        self,
        file_paths: Sequence[str],
        transform: Optional[Callable[[PngImageFile], torch.Tensor]] = None,
    ) -> None:
        self.file_paths = file_paths
        self.transform = transform if transform is not None else ToTensor()

    def __len__(self) -> int:
        return len(self.file_paths)

    def __getitem__(self, index: int) -> torch.Tensor:
        image: PngImageFile
        image = Image.open(self.file_paths[index])  # type: ignore[assignment]
        image_tensor: torch.Tensor = self.transform(image)
        return image_tensor


def get_device() -> str:
    return "cuda" if torch.cuda.is_available() else "cpu"


def get_image_dataset_features(
    model: clip.model.CLIP,
    device: str,
    dataset: Dataset[torch.Tensor],
) -> torch.Tensor:
    all_features = []
    with torch.no_grad():
        for images in tqdm(DataLoader(dataset, batch_size=64)):
            features = model.encode_image(images.to(device))
            all_features.append(features)
    return torch.cat(
        all_features,
    ).cpu()  # pylint: disable=no-member


def get_image_features(
    model: torch.Tensor,
    preprocess: Compose,
    device: str,
    file_paths: Sequence[str],
) -> torch.Tensor:
    dataset = ImageOnlyDataset(file_paths, transform=preprocess)
    return get_image_dataset_features(model, device, dataset)


def get_text_features(
    model: clip.model.CLIP,
    device: str,
    text: Union[str, Sequence[str]],
) -> torch.Tensor:
    tokens = clip.tokenize(text).to(device)
    with torch.no_grad():
        text_features: torch.Tensor = model.encode_text(tokens)
    return text_features


def text_similarity(
    text: str,
    file_paths: Sequence[str],
    model_name: Optional[str] = None,
) -> torch.Tensor:
    """
    Return similarities between a text and a sequence of images.

    The similarities are given as a Tensor containing the sequence of
    cosine similarities between an encoding of `text` and each encoding
    of the images at `file_paths`.
    """
    if model_name is None:
        model_name = "ViT-B/32"
    device = get_device()

    model, preprocess = clip.load(
        model_name,
        device=device,
    )

    text_features = get_text_features(model, device, text)
    image_features = get_image_features(model, preprocess, device, file_paths)

    image_features /= image_features.norm(  # type: ignore[no-untyped-call]
        dim=-1,
        keepdim=True,
    )
    text_features /= text_features.norm(  # type: ignore[no-untyped-call]
        dim=-1,
        keepdim=True,
    )
    return (text_features @ image_features.T)[0]


def image_similarity(
    image_filepath: str,
    file_paths: Sequence[str],
    model_name: Optional[str] = None,
) -> torch.Tensor:
    """
    Return similarities between a target image and a sequence of images.

    The similarities are given as a Tensor containing the sequence of
    cosine similarities between an encoding of the image at
    `image_filepath` each encoding of the images at `file_paths`.
    """
    if model_name is None:
        model_name = "ViT-B/32"
    device = get_device()
    model, preprocess = clip.load(model_name, device=device)

    image_features = get_image_features(
        model,
        preprocess,
        device,
        [image_filepath, *file_paths],
    )

    image_features /= image_features.norm(  # type: ignore[no-untyped-call]
        dim=-1,
        keepdim=True,
    )
    return (image_features[:1] @ image_features[1:].T)[0]
