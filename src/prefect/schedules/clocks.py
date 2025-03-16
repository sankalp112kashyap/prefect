from datetime import datetime, timedelta
from typing import Any, Iterable, List, Union

import pendulum
import pytz
from croniter import croniter


class ClockEvent:
    def __init__(self, start_time: datetime, parameter_defaults: dict = None, labels: List[str] = None) -> None:
        self.start_time = start_time
        self.parameter_defaults = parameter_defaults or {}
        self.labels = labels

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (ClockEvent, datetime)):
            return False
        if isinstance(other, datetime):
            return self.start_time == other
        return (
            self.start_time == other.start_time
            and self.parameter_defaults == other.parameter_defaults
            and self.labels == other.labels
        )

    def __gt__(self, other: Union[datetime, "ClockEvent"]) -> bool:
        if not isinstance(other, (ClockEvent, datetime)):
            raise TypeError(f"'>' not supported between instances of 'ClockEvent' and {type(other).__name__}")
        return self.start_time > other

    def __lt__(self, other: Union[datetime, "ClockEvent"]) -> bool:
        if not isinstance(other, (ClockEvent, datetime)):
            raise TypeError(f"'<' not supported between instances of 'ClockEvent' and {type(other).__name__}")
        return self.start_time < other


class Clock:
    def __init__(self, start_date: datetime = None, end_date: datetime = None, parameter_defaults: dict = None, labels: List[str] = None):
        self.start_date = pendulum.instance(start_date) if start_date else None
        self.end_date = pendulum.instance(end_date) if end_date else None
        self.parameter_defaults = parameter_defaults or {}
        self.labels = labels

    def events(self, after: datetime = None) -> Iterable[ClockEvent]:
        raise NotImplementedError("Must be implemented on Clock subclasses")


class IntervalClock(Clock):
    def __init__(self, interval: timedelta, start_date: datetime = None, end_date: datetime = None, parameter_defaults: dict = None, labels: List[str] = None):
        if not isinstance(interval, timedelta) or interval.total_seconds() <= 0:
            raise ValueError("Interval must be a positive timedelta.")
        self.interval = interval
        super().__init__(start_date, end_date, parameter_defaults, labels)

    def events(self, after: datetime = None) -> Iterable[ClockEvent]:
        after = pendulum.instance(after or pendulum.now("utc"))
        start_date = self.start_date or pendulum.datetime(2019, 1, 1)
        skip = max(0, (after - start_date).total_seconds() / self.interval.total_seconds())
        skip = int(skip) + 1 if int(skip) == skip else int(skip + 1)
        interval = self.interval * skip

        while True:
            days, seconds = interval.days, interval.total_seconds() - (days * 86400)
            next_date = start_date.add(days=days, seconds=seconds)
            if next_date < after:
                interval += self.interval
                continue
            if self.end_date and next_date > self.end_date:
                break
            yield ClockEvent(next_date, self.parameter_defaults, self.labels)
            interval += self.interval


class CronClock(Clock):
    def __init__(self, cron: str, start_date: datetime = None, end_date: datetime = None, parameter_defaults: dict = None, labels: List[str] = None, day_or: bool = None):
        if not croniter.is_valid(cron):
            raise ValueError(f"Invalid cron string: {cron}")
        self.cron = cron
        self.day_or = day_or if day_or is not None else True
        super().__init__(start_date, end_date, parameter_defaults, labels)

    def events(self, after: datetime = None) -> Iterable[ClockEvent]:
        tz = getattr(self.start_date, "tz", "UTC")
        after = pendulum.instance(after or pendulum.now(tz)).in_tz(tz)
        if self.start_date:
            after = max(after, self.start_date - timedelta(seconds=1))
        after_localized = pytz.timezone(after.tz.name).localize(after.replace(microsecond=0) + timedelta(seconds=1))
        cron = croniter(self.cron, after_localized, day_or=self.day_or)
        dates = set()

        while True:
            next_date = pendulum.instance(cron.get_next(datetime))
            if next_date.in_tz("UTC") == after.in_tz("UTC") or next_date in dates:
                next_date = pendulum.instance(cron.get_next(datetime))
            if self.end_date and next_date > self.end_date:
                break
            dates.add(next_date)
            yield ClockEvent(next_date, self.parameter_defaults, self.labels)


class DatesClock(Clock):
    def __init__(self, dates: List[datetime], parameter_defaults: dict = None, labels: List[str] = None):
        super().__init__(min(dates), max(dates), parameter_defaults, labels)
        self.dates = dates

    def events(self, after: datetime = None) -> Iterable[ClockEvent]:
        after = pendulum.instance(after or pendulum.now("UTC"))
        yield from (ClockEvent(date, self.parameter_defaults, self.labels) for date in sorted(self.dates) if date > after)
