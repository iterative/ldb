class Format:
    AUTO = "auto-detect"
    STRICT = "strict-pairs"
    BARE = "bare-pairs"
    ANNOT = "annotation-only"
    INFER = "tensorflow-inferred"
    LABEL_STUDIO = "label-studio"


INSTANTIATE_FORMATS = {
    "strict": Format.STRICT,
    Format.STRICT: Format.STRICT,
    "bare": Format.BARE,
    Format.BARE: Format.BARE,
    "infer": Format.INFER,
    Format.INFER: Format.INFER,
    "annot": Format.ANNOT,
    Format.ANNOT: Format.ANNOT,
    Format.LABEL_STUDIO: Format.LABEL_STUDIO,
}
INDEX_FORMATS = {
    "auto": Format.AUTO,
    Format.AUTO: Format.AUTO,
    **INSTANTIATE_FORMATS,
}
