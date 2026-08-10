import typing as t
import datetime


def list_executions(
    domain_name: str,
    closed: bool = False,
    workflow_name: t.Union[str, None] = None,
    workflow_version: t.Union[str, None] = None,
    earliest_started: t.Union[datetime.datetime, None] = None,
) -> t.Generator[t.Tuple[str, str, str, str, str], None, None]:
    import swf_typed

    if workflow_version:
        workflow_filter = swf_typed.WorkflowTypeExecutionFilter(
            workflow=swf_typed.WorkflowId(name=workflow_name, version=workflow_version),
        )
    elif workflow_name:
        workflow_filter = swf_typed.WorkflowTypeExecutionFilter(
            workflow=swf_typed.WorkflowIdFilter(name=workflow_name),
        )
    else:
        workflow_filter = None

    if earliest_started is None:
        earliest_started = (
            datetime.datetime.now(tz=datetime.timezone.utc)
            - datetime.timedelta(hours=24)
        )
    time_filter = swf_typed.StartTimeExecutionFilter(earliest=earliest_started)

    if closed:
        executions = swf_typed.list_closed_executions(
            domain=domain_name, time_filter=time_filter, property_filter=workflow_filter
        )
    else:
        executions = swf_typed.list_open_executions(
            domain=domain_name,
            started_filter=time_filter,
            property_filter=workflow_filter,
        )

    for execution in executions:
        yield (
            execution.execution.id,
            execution.execution.run_id,
            execution.status.value,
            execution.workflow.name,
            execution.workflow.version,
        )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="List executions")
    parser.add_argument("-d", "--domain", required=True, help="SWF domain name")
    parser.add_argument("-c", "--closed", action="store_true", help="list closed")
    parser.add_argument("-w", "--workflow", help="SWF workflow name")
    parser.add_argument("--workflow-version", help="SWF workflow version")
    parser.add_argument(
        "-a", "--after", help="earliest execution start time; default: 24 hours ago"
    )
    args = parser.parse_args()

    executions = list_executions(
        domain_name=args.domain,
        closed=args.closed,
        workflow_name=args.workflow,
        workflow_version=args.workflow_version,
        earliest_started=args.after and datetime.datetime.fromisoformat(args.after),
    )

    for exwfid, exrid, exstatus, wfname, wfver in executions:
        print(exwfid, exrid, exstatus, wfname, wfver, sep="\t")


if __name__ == "__main__":
    main()
