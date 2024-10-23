class DatabasedError(Exception):
    """General exception class."""


class DatabaseNotConnectedError(DatabasedError):
    pass


class DatabaseAlreadyConnectedError(DatabasedError):
    pass


class SessionAlreadyOpenError(DatabasedError):
    pass


class SessionNotOpenError(DatabasedError):
    pass
