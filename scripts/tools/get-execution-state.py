import typing as t


def get_execution_state(
    domain_name: str,
    workflow_id: str,
    run_id: str,
) -> t.Tuple[str, str, str, str, t.Generator[t.Tuple[str, str, str], None, None]]:
    import swf_typed

    ref = swf_typed.ExecutionId(id=workflow_id, run_id=run_id)
    events = swf_typed.get_execution_history(ref, domain=domain_name)
    state = swf_typed.build_state(events)
    return (
        state.status.value,
        (
            state.result
            if state.status == swf_typed.ExecutionStatus.completed
            else f"[{state.failure_reason}] {state.stop_details}"
        ),
        state.workflow.name,
        state.workflow.version,
        ((task.id, task.status.name, task.failure_reason) for task in state.tasks),
    )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Build execution state")
    parser.add_argument("-d", "--domain", required=True, help="SWF domain name")
    parser.add_argument("-w", "--workflow", help="execution workflow ID")
    parser.add_argument("-r", "--run", help="execution run ID")
    args = parser.parse_args()

    status, result, wf_name, wf_version, tasks = get_execution_state(
        domain_name=args.domain, workflow_id=args.workflow, run_id=args.run
    )

    print("status:", status)
    print("workflow:", wf_name, "@", wf_version)
    print("result:", result)
    print("\ntasks:")

    for tid, tstatus, treason in tasks:
        print(tid, tstatus, *([treason] if treason else []), sep=", ")


if __name__ == "__main__":
    main()
