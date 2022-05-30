import requests
from time import time

from async_client import AsyncClient


def test_speed_improvement():
    reqs = [{'method': 'get', 'url': f'https://pokeapi.co/api/v2/pokemon/{n}'} for n in range(1, 151)]

    generic_start_time = time()
    with requests.Session() as session:
        [session.request(**req) for req in reqs]
    generic_time_diff = time() - generic_start_time

    async_start_time = time()
    session = AsyncClient()
    session.execute(reqs)
    async_time_diff = time() - async_start_time

    assert async_time_diff < generic_time_diff
