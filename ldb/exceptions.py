class LDBException(Exception):
    pass


class LDBInstanceNotFoundError(LDBException):
    pass


class RecordNotFoundError(LDBException):
    pass


class CollectionNotFoundError(RecordNotFoundError):
    pass


class DatasetNotFoundError(RecordNotFoundError):
    pass


class DatasetVersionNotFoundError(RecordNotFoundError):
    pass


class DataObjectNotFoundError(RecordNotFoundError):
    pass


class AnnotationNotFoundError(RecordNotFoundError):
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
