"""Common models and methods."""

import socket
import datetime
import contextlib
import typing as t
import concurrent.futures

if t.TYPE_CHECKING:
    import botocore.client

T = t.TypeVar("T")

is_deprecated_by_registration_status = {"REGISTERED": False, "DEPRECATED": True}
registration_status_by_is_deprecated = {
    v: k for k, v in is_deprecated_by_registration_status.items()
}

executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


class _Sentinel:
    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __str__(self):
        return "< not given >"

    def __bool__(self):
        return False


unset = _Sentinel()


def ensure_client(
    client: "botocore.client.BaseClient" = None,
) -> "botocore.client.BaseClient":
    if client:
        return client

    import boto3

    return boto3.client("swf")


def parse_timeout(timeout_data: str) -> t.Union[datetime.timedelta, None]:
    if timeout_data == "NONE":
        return None
    return datetime.timedelta(seconds=int(timeout_data))


def iter_paged(
    call: t.Callable[..., t.Dict[str, t.Any]],
    model: t.Callable[[t.Dict[str, t.Any]], T],
    data_key: str,
) -> t.Generator[T, None, None]:
    def iter_() -> t.Generator[T, None, None]:
        nonlocal response

        while response.get("nextPageToken"):
            future = executor.submit(call, nextPageToken=response["nextPageToken"])
            yield from (model(d) for d in response.get(data_key) or [])
            response = future.result()
        yield from (model(d) for d in response.get(data_key) or [])

    response = call()
    return iter_()


@contextlib.contextmanager
def polling_socket_timeout(
    timeout: datetime.timedelta = datetime.timedelta(seconds=70),
) -> None:
    original_timeout_seconds = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout.total_seconds())
    try:
        yield
    finally:
        socket.setdefaulttimeout(original_timeout_seconds)
