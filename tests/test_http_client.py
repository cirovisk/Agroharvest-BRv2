"""
Testes para o ResilientHTTPClient.

Valida o mecanismo de retries com backoff exponencial,
tratamento de erros transientes e fallback de SSL.
"""

import time
from unittest.mock import patch, MagicMock

import pytest
import requests

from pipeline.http_client import ResilientHTTPClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    """Cliente com retry rápido para testes (backoff mínimo)."""
    return ResilientHTTPClient(
        max_retries=3,
        backoff_base=0.01,  # 10ms para testes rápidos
        backoff_factor=2.0,
        timeout=5,
    )


@pytest.fixture
def client_ssl():
    """Cliente com SSL fallback habilitado."""
    return ResilientHTTPClient(
        max_retries=2,
        backoff_base=0.01,
        ssl_fallback=True,
        timeout=5,
    )


# ---------------------------------------------------------------------------
# Sucesso na Primeira Tentativa
# ---------------------------------------------------------------------------

class TestSuccessPath:
    def test_get_success(self, client):
        """GET com sucesso retorna Response sem retries."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200

        with patch.object(client._session, "request", return_value=mock_response) as mock_req:
            result = client.get("https://api.example.com/data")

        assert result.status_code == 200
        assert mock_req.call_count == 1

    def test_post_success(self, client):
        """POST com sucesso retorna Response sem retries."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200

        with patch.object(client._session, "request", return_value=mock_response) as mock_req:
            result = client.post("https://api.example.com/data", data={"key": "val"})

        assert result.status_code == 200
        assert mock_req.call_count == 1


# ---------------------------------------------------------------------------
# Retries em Erros Transientes
# ---------------------------------------------------------------------------

class TestRetryBehavior:
    def test_retry_on_500_then_success(self, client):
        """Retry em HTTP 500, sucesso na segunda tentativa."""
        resp_500 = MagicMock(spec=requests.Response)
        resp_500.status_code = 500

        resp_200 = MagicMock(spec=requests.Response)
        resp_200.status_code = 200

        with patch.object(client._session, "request", side_effect=[resp_500, resp_200]) as mock_req:
            result = client.get("https://api.gov.br/data")

        assert result.status_code == 200
        assert mock_req.call_count == 2

    def test_retry_on_503_then_success(self, client):
        """Retry em HTTP 503 (Service Unavailable)."""
        resp_503 = MagicMock(spec=requests.Response)
        resp_503.status_code = 503

        resp_200 = MagicMock(spec=requests.Response)
        resp_200.status_code = 200

        with patch.object(client._session, "request", side_effect=[resp_503, resp_200]) as mock_req:
            result = client.get("https://api.ibge.gov.br/data")

        assert result.status_code == 200
        assert mock_req.call_count == 2

    def test_retry_on_429_rate_limit(self, client):
        """Retry em HTTP 429 (Too Many Requests)."""
        resp_429 = MagicMock(spec=requests.Response)
        resp_429.status_code = 429

        resp_200 = MagicMock(spec=requests.Response)
        resp_200.status_code = 200

        with patch.object(client._session, "request", side_effect=[resp_429, resp_200]) as mock_req:
            result = client.get("https://api.conab.gov.br/data")

        assert result.status_code == 200
        assert mock_req.call_count == 2

    def test_retry_on_connection_error(self, client):
        """Retry em ConnectionError."""
        resp_200 = MagicMock(spec=requests.Response)
        resp_200.status_code = 200

        with patch.object(
            client._session, "request",
            side_effect=[requests.exceptions.ConnectionError("conn refused"), resp_200]
        ) as mock_req:
            result = client.get("https://api.gov.br/data")

        assert result.status_code == 200
        assert mock_req.call_count == 2

    def test_retry_on_timeout(self, client):
        """Retry em Timeout."""
        resp_200 = MagicMock(spec=requests.Response)
        resp_200.status_code = 200

        with patch.object(
            client._session, "request",
            side_effect=[requests.exceptions.Timeout("timed out"), resp_200]
        ) as mock_req:
            result = client.get("https://api.gov.br/data")

        assert result.status_code == 200
        assert mock_req.call_count == 2


# ---------------------------------------------------------------------------
# Exaustão de Retries
# ---------------------------------------------------------------------------

class TestRetryExhaustion:
    def test_raises_after_max_retries_connection(self, client):
        """ConnectionError persistente: levanta após esgotar retries."""
        with patch.object(
            client._session, "request",
            side_effect=requests.exceptions.ConnectionError("always fail")
        ):
            with pytest.raises(requests.exceptions.ConnectionError):
                client.get("https://api.gov.br/data")

    def test_raises_after_max_retries_timeout(self, client):
        """Timeout persistente: levanta após esgotar retries."""
        with patch.object(
            client._session, "request",
            side_effect=requests.exceptions.Timeout("always timeout")
        ):
            with pytest.raises(requests.exceptions.Timeout):
                client.get("https://api.gov.br/data")

    def test_raises_after_max_retries_500(self, client):
        """HTTP 500 persistente: levanta HTTPError após esgotar retries."""
        resp_500 = MagicMock(spec=requests.Response)
        resp_500.status_code = 500
        resp_500.raise_for_status = MagicMock(
            side_effect=requests.exceptions.HTTPError("500 Server Error")
        )

        with patch.object(
            client._session, "request",
            return_value=resp_500
        ):
            with pytest.raises(requests.exceptions.HTTPError):
                client.get("https://api.gov.br/data")


# ---------------------------------------------------------------------------
# Erros Não-Retryáveis
# ---------------------------------------------------------------------------

class TestNonRetryableErrors:
    def test_invalid_url_no_retry(self, client):
        """InvalidURL não é retryável — falha imediatamente."""
        with patch.object(
            client._session, "request",
            side_effect=requests.exceptions.InvalidURL("bad url")
        ) as mock_req:
            with pytest.raises(requests.exceptions.InvalidURL):
                client.get("not-a-url")

        assert mock_req.call_count == 1

    def test_404_no_retry(self, client):
        """HTTP 404 não está na lista de retryable — levanta na primeira."""
        resp_404 = MagicMock(spec=requests.Response)
        resp_404.status_code = 404
        resp_404.raise_for_status = MagicMock(
            side_effect=requests.exceptions.HTTPError("404 Not Found")
        )

        with patch.object(client._session, "request", return_value=resp_404) as mock_req:
            with pytest.raises(requests.exceptions.HTTPError):
                client.get("https://api.gov.br/not-found")

        assert mock_req.call_count == 1


# ---------------------------------------------------------------------------
# Backoff Exponencial
# ---------------------------------------------------------------------------

class TestBackoffTiming:
    def test_backoff_delays_increase(self, client):
        """Verifica que os delays de backoff crescem exponencialmente."""
        assert client._calc_delay(0) == pytest.approx(0.01, rel=0.1)  # base * 2^0 = 0.01
        assert client._calc_delay(1) == pytest.approx(0.02, rel=0.1)  # base * 2^1 = 0.02
        assert client._calc_delay(2) == pytest.approx(0.04, rel=0.1)  # base * 2^2 = 0.04

    def test_backoff_cap(self):
        """Verifica que o delay não excede backoff_max."""
        client = ResilientHTTPClient(backoff_base=2.0, backoff_factor=2.0, backoff_max=10.0)
        assert client._calc_delay(0) == 2.0   # 2.0 * 2^0 = 2.0
        assert client._calc_delay(1) == 4.0   # 2.0 * 2^1 = 4.0
        assert client._calc_delay(2) == 8.0   # 2.0 * 2^2 = 8.0
        assert client._calc_delay(3) == 10.0  # 2.0 * 2^3 = 16.0, capped at 10.0
        assert client._calc_delay(10) == 10.0 # Ainda capped


# ---------------------------------------------------------------------------
# SSL Fallback
# ---------------------------------------------------------------------------

class TestSSLFallback:
    def test_ssl_fallback_retries_without_verify(self, client_ssl):
        """SSL error com fallback habilitado retenta sem verificação."""
        resp_200 = MagicMock(spec=requests.Response)
        resp_200.status_code = 200

        # Primeira chamada com verify=True falha com SSLError
        # Segunda chamada (fallback) com verify=False sucede
        call_count = 0
        def side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if kwargs.get("verify", True):
                raise requests.exceptions.SSLError("SSL handshake failed")
            return resp_200

        with patch.object(client_ssl._session, "request", side_effect=side_effect):
            result = client_ssl.get("https://dados.agricultura.gov.br/file.csv")

        assert result.status_code == 200

    def test_ssl_no_fallback_raises(self, client):
        """SSL error SEM fallback habilitado levanta imediatamente."""
        with patch.object(
            client._session, "request",
            side_effect=requests.exceptions.SSLError("SSL failed")
        ):
            with pytest.raises(requests.exceptions.SSLError):
                client.get("https://dados.agricultura.gov.br/file.csv")


# ---------------------------------------------------------------------------
# Download Streaming
# ---------------------------------------------------------------------------

class TestDownload:
    def test_download_saves_file(self, client, tmp_path):
        """Download salva conteúdo corretamente no arquivo."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.iter_content = MagicMock(return_value=[b"chunk1", b"chunk2"])
        mock_response.raise_for_status = MagicMock()

        dest = str(tmp_path / "test_file.csv")

        with patch.object(client._session, "request", return_value=mock_response):
            result = client.download("https://api.gov.br/file.csv", dest)

        assert result == dest
        with open(dest, "rb") as f:
            assert f.read() == b"chunk1chunk2"


# ---------------------------------------------------------------------------
# Configuração Customizada
# ---------------------------------------------------------------------------

class TestCustomConfig:
    def test_custom_timeout(self):
        """Timeout customizado é usado na requisição."""
        client = ResilientHTTPClient(timeout=120)
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200

        with patch.object(client._session, "request", return_value=mock_response) as mock_req:
            client.get("https://api.gov.br/data")

        _, kwargs = mock_req.call_args
        assert kwargs["timeout"] == 120

    def test_override_timeout_per_call(self):
        """Timeout pode ser sobreescrito por chamada."""
        client = ResilientHTTPClient(timeout=60)
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200

        with patch.object(client._session, "request", return_value=mock_response) as mock_req:
            client.get("https://api.gov.br/data", timeout=300)

        _, kwargs = mock_req.call_args
        assert kwargs["timeout"] == 300

    def test_custom_headers(self):
        """Headers customizados são aplicados na sessão."""
        headers = {"User-Agent": "AgroHarvest/1.0"}
        client = ResilientHTTPClient(headers=headers)
        assert client._session.headers["User-Agent"] == "AgroHarvest/1.0"
