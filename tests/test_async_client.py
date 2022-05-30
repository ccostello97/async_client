"""Unit tests for Async Client module."""

from time import time

import requests

from async_client import AsyncClient


def test_speed_improvement() -> None:
    """Test that there is speed improvement from generic `requests` library.

    :return: None
    """
    reqs = [
        {"method": "get", "url": f"https://pokeapi.co/api/v2/pokemon/{n}"}
        for n in range(1, 151)
    ]

    generic_start_time = time()
    with requests.Session() as session:
        for req in reqs:
            session.request(**req)  # type: ignore
    generic_time_diff = time() - generic_start_time

    async_start_time = time()
    async_client = AsyncClient()
    async_client.execute(reqs)
    async_time_diff = time() - async_start_time

    assert async_time_diff < generic_time_diff
