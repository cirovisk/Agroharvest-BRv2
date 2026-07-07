"""
HTTP Client resiliente com retry + backoff exponencial.

Centraliza todas as requisições HTTP do pipeline em um único lugar,
garantindo que falhas transientes de APIs governamentais
(CONAB, MAPA, IBGE/SIDRA) não quebrem a ingestão.

Política de retries:
  - Backoff exponencial: 2s → 4s → 8s (configurável)
  - Retenta em: ConnectionError, Timeout, 429, 500, 502, 503, 504
  - Fallback de SSL opcional (MAPA/SIPEAGRO frequentemente falha SSL)
"""

import logging
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

# Status codes que indicam falha transiente do servidor
DEFAULT_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class ResilientHTTPClient:
    """
    Cliente HTTP com retry automático e backoff exponencial.

    Uso típico (via BaseSource):
        response = self.http.get("https://api.ibge.gov.br/...", timeout=30)

    Para downloads grandes em streaming:
        self.http.download("https://dados.agricultura.gov.br/...", "data/file.csv")
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_base: float = 2.0,
        backoff_factor: float = 2.0,
        backoff_max: float = 30.0,
        timeout: int = 60,
        retryable_status_codes: frozenset[int] = DEFAULT_RETRYABLE_STATUS_CODES,
        ssl_fallback: bool = False,
        headers: dict[str, str] | None = None,
    ):
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_factor = backoff_factor
        self.backoff_max = backoff_max
        self.timeout = timeout
        self.retryable_status_codes = retryable_status_codes
        self.ssl_fallback = ssl_fallback

        self._session = requests.Session()
        if headers:
            self._session.headers.update(headers)

        # Configure the urllib3 adapter with connection pooling
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
        )
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def _calc_delay(self, attempt: int) -> float:
        """Calcula o delay com backoff exponencial: base * factor^attempt."""
        delay = self.backoff_base * (self.backoff_factor**attempt)
        return min(delay, self.backoff_max)

    def _is_retryable(self, exc: Exception | None = None, status_code: int | None = None) -> bool:
        """Determine whether the error is transient and should be retried."""
        if exc is not None:
            return isinstance(
                exc,
                (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError,
                ),
            )
        if status_code is not None:
            return status_code in self.retryable_status_codes
        return False

    def request(
        self,
        method: str,
        url: str,
        timeout: int | None = None,
        verify: bool = True,
        **kwargs: Any,
    ) -> requests.Response:
        """
        Executa uma requisição HTTP com retries e backoff exponencial.

        Args:
            method: Método HTTP (GET, POST, etc.)
            url: URL alvo
            timeout: Timeout em segundos (usa o default da instância se não informado)
            verify: Verificação SSL (True por padrão)
            **kwargs: Argumentos extras passados ao requests (headers, data, params, etc.)

        Returns:
            requests.Response

        Raises:
            requests.exceptions.HTTPError: Após esgotar todas as tentativas
            requests.exceptions.RequestException: Para erros não-retryáveis
        """
        effective_timeout = timeout if timeout is not None else self.timeout
        last_exception: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    timeout=effective_timeout,
                    verify=verify,
                    **kwargs,
                )

                # If the status is retryable and attempts remain, try again
                if self._is_retryable(status_code=response.status_code) and attempt < self.max_retries:
                    delay = self._calc_delay(attempt)
                    log.warning(
                        f"[Retry {attempt + 1}/{self.max_retries}] "
                        f"HTTP {response.status_code} em {url} — "
                        f"retentando em {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    continue

                # If the status is retryable but attempts are exhausted, raise the error
                response.raise_for_status()
                return response

            except requests.exceptions.SSLError as ssl_err:
                if self.ssl_fallback and verify:
                    log.warning(f"SSL falhou para {url}: {ssl_err}. Retentando sem verificação SSL...")
                    # Try once without SSL (does not count as a normal retry)
                    import urllib3

                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                    return self.request(method, url, timeout=effective_timeout, verify=False, **kwargs)
                last_exception = ssl_err
                break  # SSL sem fallback não é retryável

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = self._calc_delay(attempt)
                    log.warning(
                        f"[Retry {attempt + 1}/{self.max_retries}] "
                        f"{type(e).__name__} em {url} — "
                        f"retentando em {delay:.1f}s..."
                    )
                    time.sleep(delay)
                else:
                    log.error(f"Falha definitiva após {self.max_retries + 1} tentativa(s) em {url}: {e}")

            except requests.exceptions.RequestException as e:
                # Non-retryable errors (for example, InvalidURL)
                last_exception = e
                break

        # Esgotou retries
        raise last_exception  # type: ignore[misc]

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        """GET com retries."""
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        """POST com retries."""
        return self.request("POST", url, **kwargs)

    def download(
        self,
        url: str,
        dest_path: str,
        chunk_size: int = 8192,
        **kwargs: Any,
    ) -> str:
        """
        Download em streaming com retries.
        Salva o conteúdo diretamente no arquivo de destino.

        Args:
            url: URL do arquivo
            dest_path: Caminho local de destino
            chunk_size: Tamanho dos chunks (default 8KB)
            **kwargs: Argumentos extras (headers, verify, timeout, etc.)

        Returns:
            Caminho do arquivo salvo
        """
        kwargs["stream"] = True
        response = self.request("GET", url, **kwargs)
        response.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)

        log.info(f"Download concluído: {dest_path}")
        return dest_path
