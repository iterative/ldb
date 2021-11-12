def left_truncate(item: str, max_len=36) -> str:
    return (
        item
        if len(item) <= max_len
        else "..." + item[-(max_len - 3) :]  # noqa: E203
    )
