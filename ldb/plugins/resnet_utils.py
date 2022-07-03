from typing import Optional, Sequence, TypeVar

import torch
from PIL import Image
from torch.nn import AdaptiveAvgPool2d, CosineSimilarity, Module
from torchvision import models, transforms
from torchvision.models.feature_extraction import create_feature_extractor
from torchvision.transforms import InterpolationMode

T = TypeVar("T", bound="ResNetLayer")

MODELS = {
    "18": models.resnet18,
    "34": models.resnet34,
    "50": models.resnet50,
    "101": models.resnet101,
    "152": models.resnet152,
}
BLOCK_EXPANSION = {
    "18": 1,
    "34": 1,
    "50": 4,
    "101": 4,
    "152": 4,
}
RETURN_NODES = {
    "layer1": "layer1",
    "layer2": "layer2",
    "layer3": "layer3",
    "layer4": "layer4",
}
LAYER_NAMES = ("layer1", "layer2", "layer3", "layer4")

preprocess = transforms.Compose(
    [
        transforms.Resize(256, interpolation=InterpolationMode.BICUBIC),
        transforms.CenterCrop(224),
        (lambda x: x.convert("RGB")),
        transforms.ToTensor(),
        transforms.Normalize(
            mean=[0.485, 0.456, 0.406],
            std=[0.229, 0.224, 0.225],
        ),
    ],
)
cos_sim = CosineSimilarity()


class ResNetLayer(Module):
    def __init__(self, model_name: str, layer_index: int) -> None:
        super().__init__()
        self.model_name = model_name
        self.layer_index = layer_index
        self.model = MODELS[self.model_name](pretrained=True)
        self.body = create_feature_extractor(
            self.model,
            return_nodes=RETURN_NODES,
        )
        self.avgpool = AdaptiveAvgPool2d((1, 1))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        features = self.body(x)
        x = self.avgpool(features[LAYER_NAMES[self.layer_index]])
        x = torch.flatten(x, 1)
        return x

    def eval(self: T) -> T:
        self.model.eval()
        return self


def image_layer_similarity(
    image_file_path: str,
    file_paths: Sequence[str],
    model_name: str = "18",
    layer_index: Optional[int] = None,
) -> torch.Tensor:
    if layer_index is None:
        model = MODELS[model_name](pretrained=True)
    else:
        model = ResNetLayer(model_name, layer_index)
    model.eval()

    all_file_paths = [image_file_path, *file_paths]
    input_tensors = [preprocess(Image.open(x)) for x in all_file_paths]
    input_batch = torch.stack(input_tensors)

    if torch.cuda.is_available():
        input_batch = input_batch.to("cuda")
        model.to("cuda")

    with torch.no_grad():
        output = model(input_batch)

    return cos_sim(output[:1], output[1:])  # type: ignore[no-any-return]
