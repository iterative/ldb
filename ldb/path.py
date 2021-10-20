from pathlib import PurePath


class GlobalDir:
    CONFIG = PurePath(".ldb")
    DEFAULT_INSTANCE = CONFIG / "personal_instance"


class InstanceDir:
    DATA_OBJECT_INFO = PurePath("data_object_info")
    DATASETS = PurePath("datasets")
    OBJECTS = PurePath("objects")
    ANNOTATIONS = OBJECTS / "annotations"
    COLLECTIONS = OBJECTS / "collections"
    DATASET_VERSIONS = OBJECTS / "dataset_versions"


INSTANCE_DIRS = (
    InstanceDir.DATA_OBJECT_INFO,
    InstanceDir.DATASETS,
    InstanceDir.OBJECTS,
    InstanceDir.ANNOTATIONS,
    InstanceDir.COLLECTIONS,
    InstanceDir.DATASET_VERSIONS,
)


class Filename:
    CONFIG = "config"
    STORAGE = "storage"
