import os
from typing import Optional, Sequence, Union

import clip
import torch
from PIL import Image
from torch.utils.data import DataLoader, Dataset
from tqdm import tqdm


class ImageOnlyDataset(Dataset):
    def __init__(self, file_paths: Sequence[str], transform=None):
        self.file_paths = file_paths
        self.transform = transform

    def __len__(self):
        return len(self.file_paths)

    def __getitem__(self, idx):
        image = Image.open(self.file_paths[idx])
        if self.transform is not None:
            image = self.transform(image)
        return image


def get_device() -> str:
    return "cuda" if torch.cuda.is_available() else "cpu"


def get_image_dataset_features(model, device, dataset):
    all_features = []
    with torch.no_grad():
        for images in tqdm(DataLoader(dataset, batch_size=64)):
            features = model.encode_image(images.to(device))
            all_features.append(features)
    return torch.cat(all_features).cpu()


def get_image_features(model, preprocess, device, file_paths: Sequence[str]):
    dataset = ImageOnlyDataset(file_paths, transform=preprocess)
    return get_image_dataset_features(model, device, dataset)


def get_text_features(model, device, text: Union[str, Sequence[str]]):
    tokens = clip.tokenize(text).to(device)
    with torch.no_grad():
        text_features = model.encode_text(tokens)
    return text_features


def text_similarity(
    text: str,
    file_paths: Sequence[str],
    model_name: Optional[str] = None,
):
    if model_name is None:
        model_name = "ViT-B/32"
    device = get_device()

    model, preprocess = clip.load(
        model_name,
        device=device,
        download_root=os.path.expanduser("~/.cache/clip"),
    )

    text_features = get_text_features(model, device, text)
    image_features = get_image_features(model, preprocess, device, file_paths)

    image_features /= image_features.norm(dim=-1, keepdim=True)
    text_features /= text_features.norm(dim=-1, keepdim=True)
    return (text_features @ image_features.T)[0]


def image_similarity(
    image_filepath: str,
    file_paths: Sequence[str],
    model_name: Optional[str] = None,
):
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

    image_features /= image_features.norm(dim=-1, keepdim=True)
    return (image_features[:1] @ image_features[1:].T)[0]
