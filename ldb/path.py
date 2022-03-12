from pathlib import PurePath


class DirName:
    GLOBAL_BASE = ".ldb"


class GlobalDir:
    DEFAULT_INSTANCE = PurePath("private_instance")
    DEFAULT_READ_ADD_STORAGE = PurePath("read_add_storage")


class InstanceDir:
    DATA_OBJECT_INFO = PurePath("data_object_info")
    DATASETS = PurePath("datasets")
    OBJECTS = PurePath("objects")
    ANNOTATIONS = OBJECTS / "annotations"
    COLLECTIONS = OBJECTS / "collections"
    DATASET_VERSIONS = OBJECTS / "dataset_versions"
    USER_FUNCTIONS = PurePath("custom_code") / "ldb_user_functions"
    USER_FILTERS = PurePath("custom_code") / "ldb_user_filters"


class WorkspacePath:
    BASE = PurePath(".ldb_workspace")
    COLLECTION = BASE / "collection"
    DATASET = BASE / "workspace_dataset"
    TMP = BASE / "tmp"


INSTANCE_DIRS = (
    InstanceDir.DATA_OBJECT_INFO,
    InstanceDir.DATASETS,
    InstanceDir.OBJECTS,
    InstanceDir.ANNOTATIONS,
    InstanceDir.COLLECTIONS,
    InstanceDir.DATASET_VERSIONS,
    InstanceDir.USER_FUNCTIONS,
    InstanceDir.USER_FILTERS,
)


class Filename:
    CONFIG = "config"
    STORAGE = "storage"
