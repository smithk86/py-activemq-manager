from __future__ import annotations

import asyncio
import logging
import warnings
from concurrent.futures import Executor
from typing import overload, TYPE_CHECKING

import httpx
from httpx._client import ClientState

from .broker import Broker
from .errors import ActivemqManagerError


if TYPE_CHECKING:
    from typing import Any, Dict, List, Optional, Type


logger = logging.getLogger(__name__)


def _raise_for_status(response: httpx.Response) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPError as e:
        raise ActivemqManagerError(e)


class Client:
    _broker_class: Type[Broker] = Broker

    def __init__(
        self, endpoint: str, origin: str = "http://localhost:80", **http_client_kwargs
    ):
        self.endpoint = endpoint
        self.origin = origin
        self._http_client_kwargs = http_client_kwargs
        self.__http_client: Optional[httpx.AsyncClient] = None

    def __del__(self) -> None:
        if self.__http_client and self.__http_client._state is ClientState.OPENED:
            _fqcn = ".".join([type(self).__module__, type(self).__name__])
            warnings.warn(
                f"{_fqcn} for {self.endpoint} was not properly closed",
                UserWarning,
            )

    @property
    def _http_client(self) -> httpx.AsyncClient:
        if (
            self.__http_client is None
            or self.__http_client._state is ClientState.CLOSED
        ):
            self.__http_client = httpx.AsyncClient(**self._http_client_kwargs)
        return self.__http_client

    async def __aenter__(self) -> Client:
        if self._http_client._state is ClientState.UNOPENED:
            await self._http_client.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs) -> None:
        if self._http_client._state is not ClientState.CLOSED:
            await self._http_client.__aexit__(*args, **kwargs)

    async def close(self) -> None:
        await self._http_client.aclose()

    async def _request(self, type_, mbean, **kwargs) -> Any:
        payload = {"type": type_, "mbean": mbean}
        payload.update(kwargs)

        logger.debug(f"api payload: {payload}")
        try:
            _response: httpx.Response = await self._http_client.post(
                f"{self.endpoint}/api/jolokia",
                headers={"Origin": self.origin},
                json=payload,
            )
        except httpx.NetworkError as e:
            logger.exception(e)
            raise ActivemqManagerError("api call failed")

        _raise_for_status(_response)

        _payload = _response.json()
        if _payload.get("status") == 200:
            return _payload["value"]
        else:
            raise ActivemqManagerError(
                "http request returned an unexpected payload", response=_response
            )

    async def dict_request(self, type_, mbean, **kwargs) -> Dict:
        _results = await self._request(type_, mbean, **kwargs)
        if isinstance(_results, dict):
            return _results
        else:
            raise ActivemqManagerError("dictionary was expected")

    async def list_request(self, type_, mbean, **kwargs) -> List:
        _results = await self._request(type_, mbean, **kwargs)
        if isinstance(_results, list):
            return _results
        else:
            raise ActivemqManagerError("list was expected")

    async def request(self, type_, mbean, **kwargs) -> Any:
        return await self._request(type_, mbean, **kwargs)

    def broker(self, name: str = "localhost", workers: int = 10, **kwargs) -> Broker:
        return self._broker_class(self, name=name, workers=workers, **kwargs)
