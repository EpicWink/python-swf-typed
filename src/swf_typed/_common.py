"""Common models and methods."""

import abc
import socket
import datetime
import contextlib
import typing as t
import collections.abc
import concurrent.futures

from . import _exceptions

if t.TYPE_CHECKING:
    import botocore.client

T = t.TypeVar("T")

is_deprecated_by_registration_status = {"REGISTERED": False, "DEPRECATED": True}
registration_status_by_is_deprecated = {
    v: k for k, v in is_deprecated_by_registration_status.items()
}


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


class PageConsumer(collections.abc.Generator, t.Generic[T]):
    """Paged SWF API response iterator."""

    _next_page_token_key = "nextPageToken"

    def __init__(
        self,
        api_call: t.Callable[..., t.Dict[str, t.Any]],
        model: t.Callable[[t.Dict[str, t.Any]], T],
        data_key: str,
        response: t.Dict[str, t.Any],
        executor: concurrent.futures.Executor,
    ) -> None:
        """Initialise iteator.

        Args:
            api_call: AWS SWF API SDK function
            model: response model (constructor)
            data_key: response results key
            response: first response
            executor: concurrency executor
        """

        self.api_call = api_call
        self.model = model
        self.data_key = data_key
        self.response = response
        self.executor = executor

        self._i = 0
        self._future: t.Union[concurrent.futures.Future, None] = None

    @property
    def _items(self) -> t.List[t.Dict[str, t.Any]]:
        return self.response.get(self.data_key) or []

    def send(self, value: None) -> T:
        if (
            self._i == 0
            and not self._future
            and self.response.get(self._next_page_token_key)
        ):
            # Start getting next page (first iteration)
            self._future = self.executor.submit(
                self.api_call, nextPageToken=self.response[self._next_page_token_key]
            )

        if self._i >= len(self._items):
            if not self._future:
                raise StopIteration
            # Recieve next page
            self.response = self._future.result()
            self._i = 0
            if self.response.get(self._next_page_token_key):
                # Start getting next page
                self._future = self.executor.submit(
                    self.api_call,
                    nextPageToken=self.response[self._next_page_token_key],
                )
            else:
                self._future = None

        item = self._items[self._i]
        self._i += 1
        return self.model(item)

    def throw(self, typ, val=None, tb=None) -> T:
        r = self.send(None)
        self._future = None
        self.response.pop(self._next_page_token_key, None)
        self._i = len(self._items)
        return r

    def get_page(
        self,
        page_token: t.Union[str, None] = None,
        start_getting_next_page: bool = True,
    ) -> t.Tuple[t.List[T], t.Union[str, None]]:
        """Get a full page of results from SWF.

        Uses pre-fetched results if available.

        Args:
            page_token: page token
            start_getting_next_page: start fetching the next page in another
                thread

        Returns:
            page of results (structured), and next page's token
        """

        if not page_token and not self._future:
            # Use pre-fetched first response
            response = self.response

            if start_getting_next_page and self.response.get(self._next_page_token_key):
                self._future = self.executor.submit(
                    self.api_call,
                    nextPageToken=self.response[self._next_page_token_key],
                )
        elif (
            page_token
            and self._future
            and page_token == self.response.get(self._next_page_token_key)
        ):
            # Use in-flight response
            response = self._future.result()

            if start_getting_next_page:
                self.response = response
                self._i = 0
                if self.response.get(self._next_page_token_key):
                    self._future = self.executor.submit(
                        self.api_call,
                        nextPageToken=self.response[self._next_page_token_key],
                    )
        elif page_token:
            response = self.api_call(nextPageToken=page_token)
        else:
            # First page, but we're not certain if `self.response` is the first still
            response = self.api_call()

        models = [self.model(item) for item in response.get(self.data_key) or []]
        return models, response.get(self._next_page_token_key)


def ensure_client(
    client: "botocore.client.BaseClient" = None,
) -> "_exceptions.ExceptionRedirectClientWrapper":
    """Return or create SWF client."""
    if client:
        return _exceptions.redirect_exceptions_in_swf_client(client)

    import boto3

    client = boto3.client("swf")
    return _exceptions.redirect_exceptions_in_swf_client(client)


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
) -> PageConsumer[T]:
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

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    response = call()
    return PageConsumer(call, model, data_key, response, executor)


@contextlib.contextmanager
def polling_socket_timeout(
    timeout: datetime.timedelta = datetime.timedelta(seconds=70),
) -> t.Generator[None, None, None]:
    """Set socket timeout for polling in a context."""
    original_timeout_seconds = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout.total_seconds())
    try:
        yield
    finally:
        socket.setdefaulttimeout(original_timeout_seconds)
