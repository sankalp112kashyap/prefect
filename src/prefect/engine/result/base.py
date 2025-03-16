"""
Results represent Prefect Task inputs and outputs.  In particular, anytime a
Task runs, its output is encapsulated in a `Result` object.  This object retains
information about what the data is, and how to "handle" it if it needs to be
saved / retrieved at a later time (for example, if this Task requests for its
outputs to be cached or checkpointed).

An instantiated Result object has the following attributes:

- a `value`: the value of a Result represents a single piece of data
- a `safe_value`: this attribute maintains a reference to a `SafeResult` object
    which contains a "safe" representation of the `value`; for example, the
    `value` of a `SafeResult` might be a URI or filename pointing to where the
    raw data lives
- a `serializer`: an object that can serialize Python objects to bytes and
  recover them later
- a `result_handler` that holds onto the `ResultHandler` used to read / write
    the value to / from its handled representation

To distinguish between a Task that runs but does not return output from a Task
that has yet to run, Prefect also provides a `NoResult` object representing the
_absence_ of computation / data.  This is in contrast to a `Result` whose value
is `None`.
"""
import copy
import pendulum
import uuid
from typing import Any, Callable, Iterable

from prefect.engine.result_handlers import ResultHandler
from prefect.engine.serializers import PickleSerializer, Serializer
from prefect.utilities import logging


class ResultInterface:
    """
    A necessary evil so that Results can store SafeResults and NoResults
    in its attributes without pickle recursion problems.
    """
    def __eq__(self, other: Any) -> bool:
        if type(self) != type(other):
            return False
        return all(
            getattr(self, attr, object()) == getattr(other, attr, object())
            for attr in self.__dict__ if not attr.startswith("_")
        )

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {repr(self.value)}>"

    def to_result(self, result_handler: ResultHandler = None) -> "ResultInterface":
        """
        If no result handler provided, returns self. If a ResultHandler is
        provided, however, it will become the new result handler for this result.
        """
        if result_handler is not None:
            self.result_handler = result_handler
        return self

    def store_safe_value(self) -> None:
        """Performs no computation."""


class Result(ResultInterface):
    """
    A representation of the result of a Prefect task; this class contains
    information about the value of a task's result, a result handler specifying
    how to serialize or store this value securely, and a `safe_value` attribute
    which holds information about the current "safe" representation of this
    result.
    """
    def __init__(
        self,
        value: Any = None,
        result_handler: ResultHandler = None,
        validators: Iterable[Callable] = None,
        run_validators: bool = True,
        location: str = None,
        serializer: Serializer = None,
    ):
        self.value = value
        self.safe_value = NoResult
        self.result_handler = result_handler
        self.validators = validators
        self.run_validators = run_validators
        self.serializer = serializer or PickleSerializer()
        self.location = None
        self._formatter = location if not isinstance(location, (str, type(None))) else None
        if isinstance(location, str):
            self.location = location
        self.logger = logging.get_logger(type(self).__name__)

    def store_safe_value(self) -> None:
        """Populate the `safe_value` attribute with a `SafeResult` using the result handler"""
        if self.value is None or self.safe_value != NoResult:
            return
        assert isinstance(self.result_handler, ResultHandler), "Result has no ResultHandler"
        self.safe_value = SafeResult(
            value=self.result_handler.write(self.value),
            result_handler=self.result_handler
        )

    def from_value(self, value: Any) -> "Result":
        """Create a new copy of the result object with the provided value."""
        new = self.copy()
        new.location = None
        new.value = value
        return new

    def validate(self) -> bool:
        """Run validator functions and return whether the result is valid."""
        if not self.run_validators:
            self.logger.warning(
                "A Result's validate method has been called, but its run_validators "
                "attribute is False. Prefect will not honor the validators without "
                "run_validators=True, so please change it if you expect validation "
                "to occur automatically for this Result in your pipeline."
            )
        if not self.validators:
            return True
        return all(validation_fn(self) for validation_fn in self.validators)

    def copy(self) -> "Result":
        """Return a copy of the current result object."""
        return copy.copy(self)

    @property
    def default_location(self) -> str:
        date = pendulum.now("utc").format("Y/M/D")
        return f"{date}/{uuid.uuid4()}.prefect_result"

    def format(self, **kwargs: Any) -> "Result":
        """Format the result location with provided kwargs."""
        new = self.copy()
        if isinstance(new.location, str):
            new.location = new.location.format(**kwargs)
        elif new._formatter is not None:
            new.location = new._formatter(**kwargs)
        else:
            new.location = new.default_location
        return new

    def exists(self, location: str, **kwargs: Any) -> bool:
        """Checks whether the target result exists."""
        raise NotImplementedError(
            "Not implemented on the base Result class - if you are seeing this error you "
            "might be trying to use features that require choosing a Result subclass; "
            "see https://docs.prefect.io/core/concepts/results.html"
        )

    def read(self, location: str) -> "Result":
        """Reads from the target result."""
        raise NotImplementedError(
            "Not implemented on the base Result class - if you are seeing this error you "
            "might be trying to use features that require choosing a Result subclass; "
            "see https://docs.prefect.io/core/concepts/results.html"
        )

    def write(self, value_: Any, **kwargs: Any) -> "Result":
        """Write the result to the target location."""
        raise NotImplementedError(
            "Not implemented on the base Result class - if you are seeing this error you "
            "might be trying to use features that require choosing a Result subclass; "
            "see https://docs.prefect.io/core/concepts/results.html"
        )


class SafeResult(ResultInterface):
    """A safe representation of the result of a Prefect task."""
    def __init__(self, value: Any, result_handler: ResultHandler):
        self.value = value
        self.result_handler = result_handler

    @property
    def safe_value(self) -> "SafeResult":
        return self

    def to_result(self, result_handler: ResultHandler = None) -> "ResultInterface":
        """Read the value using the result handler and return a hydrated Result."""
        if result_handler is not None:
            self.result_handler = result_handler
        value = self.result_handler.read(self.value)
        res = Result(value=value, result_handler=self.result_handler)
        res.safe_value = self
        return res


class NoResultType(SafeResult):
    """Represents the absence of computation/output."""
    def __init__(self) -> None:
        self.location = None
        super().__init__(value=None, result_handler=ResultHandler())

    def __eq__(self, other: Any) -> bool:
        return type(self) == type(other)

    def __repr__(self) -> str:
        return "<No result>"

    def __str__(self) -> str:
        return "NoResult"

    def to_result(self, result_handler: ResultHandler = None) -> "ResultInterface":
        """Returns self."""
        return self


NoResult = NoResultType()