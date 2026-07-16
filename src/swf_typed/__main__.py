"""Python interface to SWF command line application."""

TYPE_CHECKING = False
if TYPE_CHECKING:
    import typing as t
    import datetime


def _read_text_from_file(path: str) -> str:
    """Read text from a file, or stdin.

    Args:
        path: file path, or '-' for stdin

    Returns:
        file contents as text
    """

    import sys

    if path == "-":
        return sys.stdin.read()
    else:
        with open(path, mode="r", encoding="utf-8") as f:
            return f.read()


def _raw_as_sdk(x: "t.Any") -> "t.Any":
    """Convert raw built-in objects to match SDK.

    Converts data-class fields which are typed as date-time but provided as
    a string to date-time. Applied recursively.

    Args:
        x: object to convert the fields of, modified in-place

    Returns:
        provided object
    """

    import datetime
    import dataclasses

    try:
        fields = dataclasses.fields(x)
    except TypeError:
        return x

    for field in fields:
        value = getattr(x, field.name)
        if (
            field.type in (datetime.datetime, "datetime.datetime")
            and isinstance(value, str)
        ):
            setattr(x, field.name, datetime.datetime.fromisoformat(value))
        elif dataclasses.is_dataclass(value):
            setattr(x, field.name, _raw_as_sdk(value))  # recurse

    return x


def _isoformat_duration(td: "datetime.timedelta") -> str:
    """Format time-delta as ISO 8601 duration string.

    Args:
        td: time-delta to format

    Returns:
        ISO 8601 duration
    """

    if td.microseconds:
        return f"P{td.days}DT{td.seconds}.{td.microseconds:0>6d}S"
    elif td.seconds:
        return f"P{td.days}DT{td.seconds}S"
    else:
        return f"P{td.days}D"


def _build_state(
    history: "t.Union[t.List[t.Dict[str, t.Any]], t.Dict[str, t.Any]]",
    output_results: bool = False,
) -> "t.Generator[str, None, None]":
    """Build execution state from its history.

    Args:
        history: execution history (API response or its events)
        output_results: include execution and task results and stop details
            in output

    Returns:
        a generator yielding output lines of state serialise as YAML
    """

    import json

    from . import _executions, _history, _state

    events = history if isinstance(history, list) else history["events"]
    state = _state.build_state(_raw_as_sdk(_history.Event.from_api(x)) for x in events)
    del events, history

    yield f"status: {state.status.value}"
    yield f"workflow: {state.workflow.name} @ {state.workflow.version}"
    yield f"started: {state.started.isoformat(sep='T')}"

    if state.ended:
        yield f"ended: {state.ended.isoformat(sep='T')}"
        yield f"duration: {_isoformat_duration(state.ended - state.started)}"

    if output_results and state.status == _executions.ExecutionStatus.completed:
        yield f"result: {json.dumps(state.result)}"
    elif state.failure_reason or (output_results and state.stop_details):
        value = f"{state.failure_reason}" + (
            f" - {state.stop_details}" if output_results else ""
        )
        if ": " in value or "\n" in value:
            value = "'" + value.replace("'", "''") + "'"
        yield f"error: {value}"

    yield "\ntasks:"
    for task in state.tasks:
        yield f"  - id: {task.id}"
        yield f"    status: {task.status.name}"
        yield f"    scheduled: {task.scheduled.isoformat(sep='T')}"

        if task.started:
            yield f"    started: {task.scheduled.isoformat(sep='T')}"
            yield f"    enqueued: {_isoformat_duration(task.started - task.scheduled)}"

        if task.ended:
            yield f"    ended: {task.ended.isoformat(sep='T')}"
            yield f"    duration: {_isoformat_duration(task.ended - task.started)}"

        if output_results and task.status == _state.TaskStatus.completed:
            yield f"    result: {json.dumps(task.result)}"
        elif (
            task.failure_reason
            or (isinstance(task, _state.TaskState) and task.timeout_type)
            or (output_results and task.stop_details)
        ):
            failure_reason = task.failure_reason
            if failure_reason is None:
                if isinstance(task, _state.TaskState) and task.timeout_type:
                    failure_reason = task.timeout_type.value
                else:
                    failure_reason = "null"
            yield (
                f"    error: {failure_reason}"
                + (f" - {task.stop_details}" if output_results else "")
            )


def main(argv=None) -> None:
    """Run application from command line."""

    import argparse

    def build_state() -> None:
        """Build execution state from its history."""

        import json

        for line in _build_state(
            history=json.loads(_read_text_from_file(args.file)),
            output_results=args.output_results,
        ):
            print(line)

    parser = argparse.ArgumentParser(
        description="Python interface to SWF command line app",
    )

    subparsers = parser.add_subparsers(
        title="subcommands", required=True, metavar="COMMAND", help="command to run"
    )

    state_parser = subparsers.add_parser(
        name="build-state",
        help="build execution state from its history",
        description="Build execution state from its history",
    )
    state_parser.add_argument("file", help="execution history file path; '-' for stdin")
    state_parser.add_argument(
        "-R", "--output-results", action="store_true", help="include results in output"
    )
    state_parser.set_defaults(func=build_state)

    args = parser.parse_args(argv)
    args.func()


if __name__ == "__main__":
    main()
