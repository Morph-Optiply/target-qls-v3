"""QlsV2 base sink / client, built on the Hotglue SDK."""

from __future__ import annotations

import ast
import json
from base64 import b64encode
from datetime import datetime

import backoff
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink


class QlsV2Sink(HotglueSink):
    """Base sink for the QLS v2 API.

    Provides shared HTTP helpers (auth, request_api, validate_response,
    clean_payload) that individual stream sinks inherit.
    """

    # ------------------------------------------------------------------ #
    # Subclasses must define these                                         #
    # ------------------------------------------------------------------ #

    @property
    def name(self) -> str:  # type: ignore[override]
        raise NotImplementedError

    @property
    def endpoint(self) -> str:
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # URL helpers                                                          #
    # ------------------------------------------------------------------ #

    @property
    def base_url(self) -> str:
        company_id = self.config["company_id"]
        return f"https://api.pakketdienstqls.nl/v2/companies/{company_id}/"

    def url(self, endpoint: str | None = None) -> str:
        return f"{self.base_url}{endpoint or self.endpoint}"

    # ------------------------------------------------------------------ #
    # Authentication                                                       #
    # ------------------------------------------------------------------ #

    @property
    def authenticator(self) -> str:
        user = self.config.get("username")
        passwd = self.config.get("password")
        token = b64encode(f"{user}:{passwd}".encode()).decode()
        return f"Basic {token}"

    @property
    def http_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Authorization": self.authenticator,
        }

    # ------------------------------------------------------------------ #
    # HTTP layer                                                           #
    # ------------------------------------------------------------------ #

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        factor=7,
        max_tries=8,
    )
    def _request(
        self,
        http_method: str,
        endpoint: str,
        params: dict | None = None,
        request_data: dict | None = None,
    ) -> requests.Response:
        url = self.url(endpoint)
        self.logger.info(
            f"Making {http_method} request to {url} "
            f"params={params} body={request_data}"
        )
        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=self.http_headers,
            json=request_data,
        )
        self.logger.info(f"Response: {response.text}")
        self.validate_response(response)
        return response

    def request_api(
        self,
        http_method: str,
        endpoint: str | None = None,
        params: dict | None = None,
        request_data: dict | None = None,
    ) -> requests.Response:
        return self._request(http_method, endpoint, params, request_data)

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code in [429] or 500 <= response.status_code < 600:
            raise RetriableAPIError(self.response_error_message(response), response)
        elif 400 <= response.status_code < 500 and response.status_code not in [404]:
            try:
                msg = response.text
            except Exception:
                msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def response_error_message(self, response: requests.Response) -> str:
        error_type = "Client" if 400 <= response.status_code < 500 else "Server"
        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.reason} for path: {self.endpoint}"
        )

    # ------------------------------------------------------------------ #
    # Utility helpers                                                      #
    # ------------------------------------------------------------------ #

    def parse_stringified_object(self, value):
        if not isinstance(value, str):
            return value
        try:
            return ast.literal_eval(value)
        except Exception:
            return json.loads(value)

    @staticmethod
    def _clean_dict_items(d: dict) -> dict:
        return {k: v for k, v in d.items() if v not in [None, ""]}

    def clean_payload(self, item: dict) -> dict:
        item = self._clean_dict_items(item)
        output: dict = {}
        for k, v in item.items():
            if isinstance(v, datetime):
                dt_str = v.strftime("%Y-%m-%dT%H:%M:%S%z")
                if len(dt_str) > 20:
                    output[k] = f"{dt_str[:-2]}:{dt_str[-2:]}"
                else:
                    output[k] = dt_str
            elif isinstance(v, dict):
                output[k] = self.clean_payload(v)
            else:
                output[k] = v
        return output

    # ------------------------------------------------------------------ #
    # Hotglue SDK: state bookkeeping                                       #
    # ------------------------------------------------------------------ #

    def preprocess_record(self, record: dict, context: dict) -> dict:  # type: ignore[override]
        return record
