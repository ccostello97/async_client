"""Async Client class and helpers."""

from types import TracebackType
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type, Union

import asyncio
from aiohttp import ClientSession, TCPConnector
from aiolimiter import AsyncLimiter

JsonType = Dict[str, Any]
RequestsType = List[JsonType]
ResponseType = Union[JsonType, List[JsonType]]
ResponsesType = List[ResponseType]
RequestFuncType = Callable[
    [ClientSession, AsyncLimiter, JsonType], Awaitable[Any]
]


class DummyAsyncLimiter(AsyncLimiter):
    """Dummy async rate limiter for when no rate limit is specified.

    Necessary because passing a `requests_per_period` of None to the
    regular AsyncLimiter throws an exception.
    """

    def __init__(self) -> None:
        """Initialize DummyAsyncLimiter."""
        super().__init__(1.0, 1.0)

    async def __aenter__(self) -> None:
        """Enter DummyAsyncLimiter.

        :return: None
        """
        return None

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        """Exit DummyAsyncLimiter.

        :param exc_type:
        :param exc:
        :param tb:
        :return: None
        """
        return None


async def async_request(
    session: ClientSession, rate_limit: AsyncLimiter, payload: JsonType
) -> Any:
    """Make basic request.

    This function can be substituted with a
    UDF with the `request_func` argument to AsyncClient in instances
    where a custom logic is required, such as when querying APIs using
    pagination. In such cases, this function can still be used for
    individual requests inside those UDFs to handle rate limiting
    of each individual request.

    :param session: ClientSession
    :param rate_limit: AsyncLimiter
    :param payload: JsonType
    :return: Any
    """
    async with rate_limit:
        async with session.request(**payload) as response:
            response.raise_for_status()
            return await response.json()


class AsyncClient:
    """Async requests client.

    For making requests as quickly as possible while still respecting endpoint
    constraints.
    """

    def __init__(
        self,
        request_func: Optional[RequestFuncType] = None,
        requests_per_period: Optional[int] = None,
        period_seconds: Optional[int] = None,
        max_connections: Optional[int] = None,
    ) -> None:
        """Initialize the AsyncClient.

        :param request_func: user-defined function for making requests
        :param requests_per_period: max requests to make in rate limit period
        :param period_seconds: number of seconds in a rate limit period
        :param max_connections: max number of concurrent connections
        """
        self.request_func = request_func or async_request
        self.requests_per_period = requests_per_period
        self.period_seconds = period_seconds
        self.max_connections = max_connections
        self.rate_limiter = (
            AsyncLimiter(requests_per_period, period_seconds)  # type: ignore
            if requests_per_period is not None
            else DummyAsyncLimiter()
        )

    async def proc_request(
        self,
        session: ClientSession,
        request: JsonType,
        responses: ResponsesType,
    ) -> None:
        """Run `request_func` and append the response to a list of responses.

        Populates `responses` with JSONs. In the instance that a custom UDF was
        provided to query an API with paginated results, you will populate
        `responses` with lists of JSONs that can be flattened later.

        :param session: ClientSession
        :param request: JsonType
        :param responses: ResponsesType
        :return: None
        """
        response = await self.request_func(session, self.rate_limiter, request)
        responses.append(response)

    async def async_main(self, requests: RequestsType) -> ResponsesType:
        """Create tasks for every request.

        In the instance that you
        are querying a paginated API, these will only be the root request,
        the paginated requests cannot be done asynchronously. They must be
        done sequentially as each request hinges on the results of the
        previous request.

        :param requests: RequestsType
        :return: ResponsesType
        """
        responses: ResponsesType = []
        conn = TCPConnector(limit_per_host=self.max_connections or 0)
        async with ClientSession(connector=conn) as session:
            tasks = set()
            for request in requests:
                tasks.add(
                    asyncio.create_task(
                        self.proc_request(session, request, responses)
                    )
                )
            await asyncio.wait(tasks)
            return responses

    def execute(self, requests: RequestsType) -> ResponsesType:
        """Run every request task asynchronously and return a list of responses.

        :param requests: RequestsType
        :return: ResponsesType
        """
        return asyncio.run(self.async_main(requests))


__all__ = ["AsyncClient", "async_request"]
