from typing import Sequence, Union

FSProtocol = Union[str, Sequence[str]]


def first_protocol(fs_protocol: FSProtocol) -> str:
    if isinstance(fs_protocol, str):
        return fs_protocol
    return fs_protocol[0]


def has_protocol(fs_protocol: FSProtocol, protocol: str) -> bool:
    if isinstance(fs_protocol, str):
        return protocol == fs_protocol
    return protocol in fs_protocol
