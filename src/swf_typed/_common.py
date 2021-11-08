"""Common models and methods."""

import abc
import socket
import datetime
import contextlib
import typing as t
import concurrent.futures

from . import _exceptions

if t.TYPE_CHECKING:
    import botocore.client

T = t.TypeVar("T")

is_deprecated_by_registration_status = {"REGISTERED": False, "DEPRECATED": True}
registration_status_by_is_deprecated = {
    v: k for k, v in is_deprecated_by_registration_status.items()
}

executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


class _Sentinel:
    """Not-provided value sentinel."""

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __str__(self):
        return "< not given >"

    def __bool__(self):
        return False


unset = _Sentinel()


class Deserialisable(metaclass=abc.ABCMeta):
    """Deserialisable from SWF API response data."""

    @classmethod
    @abc.abstractmethod
    def from_api(cls, data: t.Dict[str, t.Any]) -> "Deserialisable":
        """Deserialise from SWF API response data."""


class Serialisable(metaclass=abc.ABCMeta):
    """Serialisable to SWF API request data."""

    @abc.abstractmethod
    def to_api(self) -> t.Dict[str, t.Any]:
        """Serialise to SWF API request data."""


class SerialisableToArguments(metaclass=abc.ABCMeta):
    """Serialisable to SWF API request arguments."""

    @abc.abstractmethod
    def get_api_args(self) -> t.Dict[str, t.Any]:
        """Serialise to SWF API request arguments."""


def ensure_client(
    client: "botocore.client.BaseClient" = None,
) -> "botocore.client.BaseClient":
    """Return or create SWF client."""
    if client:
        _exceptions.redirect_exceptions_in_swf_client(client)
        return client

    import boto3

    client = boto3.client("swf")
    _exceptions.redirect_exceptions_in_swf_client(client)
    return client


def parse_timeout(timeout_data: str) -> t.Union[datetime.timedelta, None]:
    """Parse timeout from SWF.

    Args:
        timeout_data: timeout string

    Returns:
        timeout
    """

    if timeout_data == "NONE":
        return None
    return datetime.timedelta(seconds=int(timeout_data))


def iter_paged(
    call: t.Callable[..., t.Dict[str, t.Any]],
    model: t.Callable[[t.Dict[str, t.Any]], T],
    data_key: str,
) -> t.Generator[T, None, None]:
    """Yield results from paginated method.

    Method is called immediately, then a generator is returned which yields
    results. If a pagination token is found in the response, retrieval of
    the next page is immediately scheduled (called in another thread).
    Further pages are not scheduled until the current page is consumed.

    Args:
        call: paginated method
        model: transform results (eg into data model)
        data_key: response results key

    Returns:
        method results, transformed
    """

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
    """Set socket timeout for polling in a context."""
    original_timeout_seconds = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout.total_seconds())
    try:
        yield
    finally:
        socket.setdefaulttimeout(original_timeout_seconds)
