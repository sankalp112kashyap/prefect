import copy
import pendulum
import uuid
from typing import Any, Callable, Iterable

from prefect.engine.result_handlers import ResultHandler
from prefect.engine.serializers import PickleSerializer, Serializer
from prefect.utilities import logging


class ResultInterface:
    def __eq__(self, other: Any) -> bool:
        if type(self) == type(other):
            eq = True
            for attr in self.__dict__:
                if attr.startswith("_"):
                    continue
                eq &= getattr(self, attr, object()) == getattr(other, attr, object())
            return eq
        return False

    def __repr__(self) -> str:
        val = self.value  # type: ignore
        return f"<{type(self).__name__}: {repr(val)}>"

    def to_result(self, result_handler: ResultHandler = None) -> "ResultInterface":
        if result_handler is not None:
            self.result_handler = result_handler
        return self

    def store_safe_value(self) -> None:
        pass


class Result(ResultInterface):
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
        self.location = location if isinstance(location, (str, type(None))) else None
        self._formatter = location if not isinstance(location, (str, type(None))) else None
        self.logger = logging.get_logger(type(self).__name__)

    def store_safe_value(self) -> None:
        if self.value is None:
            return
        if self.safe_value == NoResult:
            assert isinstance(self.result_handler, ResultHandler), "Result has no ResultHandler"
            value = self.result_handler.write(self.value)
            self.safe_value = SafeResult(value=value, result_handler=self.result_handler)

    def from_value(self, value: Any) -> "Result":
        new = self.copy()
        new.location = None
        new.value = value
        return new

    def validate(self) -> bool:
        if not self.run_validators:
            self.logger.warning(
                "A Result's validate method has been called, but its run_validators "
                "attribute is False. Prefect will not honor the validators without "
                "run_validators=True, so please change it if you expect validation "
                "to occur automatically for this Result in your pipeline."
            )

        if self.validators:
            for validation_fn in self.validators:
                if not validation_fn(self):
                    return False
        return True

    def copy(self) -> "Result":
        return copy.copy(self)

    @property
    def default_location(self) -> str:
        date = pendulum.now("utc").format("Y/M/D")
        return f"{date}/{uuid.uuid4()}.prefect_result"

    def format(self, **kwargs: Any) -> "Result":
        new = self.copy()
        if isinstance(new.location, str):
            new.location = new.location.format(**kwargs)
        elif new._formatter is not None:
            new.location = new._formatter(**kwargs)
        else:
            new.location = new.default_location
        return new

    def exists(self, location: str, **kwargs: Any) -> bool:
        raise NotImplementedError(
            "Not implemented on the base Result class - if you are seeing this error you "
            "might be trying to use features that require choosing a Result subclass; "
            "see https://docs.prefect.io/core/concepts/results.html"
        )

    def read(self, location: str) -> "Result":
        raise NotImplementedError(
            "Not implemented on the base Result class - if you are seeing this error you "
            "might be trying to use features that require choosing a Result subclass; "
            "see https://docs.prefect.io/core/concepts/results.html"
        )

    def write(self, value_: Any, **kwargs: Any) -> "Result":
        raise NotImplementedError(
            "Not implemented on the base Result class - if you are seeing this error you "
            "might be trying to use features that require choosing a Result subclass; "
            "see https://docs.prefect.io/core/concepts/results.html"
        )


class SafeResult(ResultInterface):
    def __init__(self, value: Any, result_handler: ResultHandler):
        self.value = value
        self.result_handler = result_handler

    @property
    def safe_value(self) -> "SafeResult":
        return self

    def to_result(self, result_handler: ResultHandler = None) -> "ResultInterface":
        if result_handler is not None:
            self.result_handler = result_handler
        value = self.result_handler.read(self.value)
        res = Result(value=value, result_handler=self.result_handler)
        res.safe_value = self
        return res


class NoResultType(SafeResult):
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
        return self


NoResult = NoResultType()
