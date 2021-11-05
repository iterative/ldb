class LDBException(Exception):
    pass


class LDBInstanceNotFoundError(LDBException):
    pass


class WorkspaceError(LDBException):
    pass
