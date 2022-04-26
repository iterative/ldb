from typing import Any

from jmespath import exceptions


class IdentifierException(exceptions.JMESPathError):
    def __init__(  # pylint: disable=super-init-not-called
        self,
        identifier: str,
    ) -> None:
        self.identifier = identifier

    def __str__(self) -> str:
        return f"{self.identifier}"


class MissingIdentifierException(IdentifierException):
    pass


class IdentifierTypeException(IdentifierException):
    def __init__(self, identifier: str, subject: Any) -> None:
        super().__init__(identifier)
        self.subject = subject

    def __str__(self) -> str:
        return (
            f"Invalid application of identifier {self.identifier}. "
            "Identifier expression subject must have a JSON Object-like type "
            "such as dict, not "
            f"{type(self.subject)}"
        )


class InvalidIdentifierExpression(IdentifierException):
    pass
