class LDBException(Exception):
    pass


class LDBInstanceNotFoundError(LDBException):
    pass


class DatasetError(LDBException):
    pass


class DatasetNotFoundError(DatasetError):
    pass


class DataObjectNotFoundError(LDBException):
    pass


class WorkspaceError(LDBException):
    pass


class WorkspaceDatasetNotFoundError(WorkspaceError):
    pass


class StorageConfigurationError(LDBException):
    pass


class NotAStorageLocationError(LDBException):
    pass


class IndexingException(LDBException):
    pass
