from .add import AddCommandBase


class TestAdd(AddCommandBase):
    COMMAND = "add"


class TestAddPhysical(AddCommandBase):
    COMMAND = "add"
    PHYSICAL = True
