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
        if field.type in (datetime.datetime, "datetime.datetime"):
            if isinstance(value, str):
                setattr(x, field.name, datetime.datetime.fromisoformat(value))
            elif isinstance(value, (int, float)):
                setattr(
                    x,
                    field.name,
                    datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc),
                )
            elif not isinstance(value, datetime.datetime):
                raise TypeError(f"Not a date-time ({field.name}): {value}")
        elif dataclasses.is_dataclass(value):
            setattr(x, field.name, _raw_as_sdk(value))  # recurse

    return x


def _build_state(
    history: "t.Union[t.List[t.Dict[str, t.Any]], t.Dict[str, t.Any]]",
) -> t.Dict[str, t.Any]:
    """Build execution state from its history.

    Args:
        history: execution history (API response or its events)

    Returns:
        execution state, as built-in (JSON-serialisable) types
    """

    from . import _history, _state

    events = history if isinstance(history, list) else history["events"]
    state = _state.build_state(_raw_as_sdk(_history.Event.from_api(x)) for x in events)
    del events, history
    return state.to_dict()


def _format_state(
    state: t.Dict[str, t.Any],
    output_results: bool = False,
) -> "t.Generator[str, None, None]":
    """Format execution state.

    Args:
        state: execution state, as built-in (JSON-deserialised) types

    Returns:
        a generator yielding output lines of state serialise as YAML
    """

    import json
    import datetime

    from . import _common, _executions, _state

    def get_duration(start: str, end: str) -> str:
        return _common.serialise_timedelta(
            datetime.datetime.fromisoformat(end)
            - datetime.datetime.fromisoformat(start)
        )

    yield f"status: {state['status']}"
    yield f"workflow: {state['workflow']['name']} @ {state['workflow']['version']}"
    yield f"started: {state['started']}"

    if state.get("ended"):
        yield f"ended: {state['ended']}"
        yield f"duration: {get_duration(state['started'], state['ended'])}"

    if (
        output_results
        and state["status"] == _executions.ExecutionStatus.completed.value
    ):
        yield f"result: {json.dumps(state.get('result'))}"
    elif state.get("failure_reason") or (output_results and state.get("stop_details")):
        value = f"{state.get('failure_reason') or '<stopped>'}" + (
            f" - {state['stop_details']}" if output_results else ""
        )
        if ": " in value or "\n" in value:
            value = "'" + value.replace("'", "''") + "'"
        yield f"error: {value}"

    yield "\ntasks:"
    task: t.Dict[str, t.Any]
    for task in state["tasks"]:
        yield f"  - id: {task['id']}"
        yield f"    status: {task['status']}"
        yield f"    scheduled: {task['scheduled']}"

        if task.get("started"):
            yield f"    started: {task['started']}"
            yield f"    enqueued: {get_duration(task['scheduled'], task['started'])}"

        if task.get("ended"):
            yield f"    ended: {task['ended']}"
            yield f"    enqueued: {get_duration(task['started'], task['ended'])}"

        if output_results and task["status"] == _state.TaskStatus.completed.value:
            yield f"    result: {json.dumps(task.get('result'))}"
        elif (
            task.get("failure_reason")
            or task.get("timeout_type")
            or (output_results and task.get("stop_details"))
        ):
            failure_reason = task.get("failure_reason")
            if failure_reason is None:
                if task.get("timeout_type"):
                    failure_reason = task["timeout_type"]
                else:
                    failure_reason = "null"
            yield (
                f"    error: {failure_reason}"
                + (f" - {task.get('stop_details')}" if output_results else "")
            )


def main(argv=None) -> None:
    """Run application from command line."""

    import argparse

    def build_state() -> None:
        """Build execution state from its history."""

        import sys
        import json

        state = _build_state(
            history=json.loads(_read_text_from_file(args.history_file)),
        )

        f = sys.stdout if args.state_file == "-" else open(args.state_file, mode="w")
        try:
            json.dump(state, f, indent=2 if f.isatty() else None)
        finally:
            if args.state_file == "-":
                f.close()

    def format_state() -> None:
        """Format execution state."""

        import json

        for line in _format_state(
            state=json.loads(_read_text_from_file(args.file)),
            output_results=args.output_results,
        ):
            print(line)

    # Common command-line arguments
    parser = argparse.ArgumentParser(
        description="Python interface to SWF command line app",
    )

    subparsers = parser.add_subparsers(
        title="subcommands", required=True, metavar="COMMAND", help="command to run"
    )

    # Build-state subcommand
    build_state_parser = subparsers.add_parser(
        name="build-state",
        help="build execution state from its history",
        description=build_state.__doc__,
    )
    build_state_parser.add_argument(
        "history_file", help="execution history file path; '-' for stdin"
    )
    build_state_parser.add_argument(
        "state_file", help="output execution state file path; '-' for stdout"
    )
    build_state_parser.set_defaults(func=build_state)

    # Format-state subcommand
    format_state_parser = subparsers.add_parser(
        name="format-state",
        help="format execution state",
        description=build_state.__doc__,
    )
    format_state_parser.add_argument(
        "file", help="execution state file path; '-' for stdin"
    )
    format_state_parser.add_argument(
        "-R", "--output-results", action="store_true", help="include results in output"
    )
    format_state_parser.set_defaults(func=format_state)

    # Parse command-line arguments, run subcommand
    args = parser.parse_args(argv)
    args.func()


if __name__ == "__main__":
    main()
