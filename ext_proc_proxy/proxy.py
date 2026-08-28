"""Core proxy implementation using aiohttp."""

import asyncio
import logging
import ssl
from typing import AsyncIterator, Optional, Set

import aiohttp
from aiohttp import ClientTimeout, web
import multidict

from ext_proc_proxy.cert_utils import create_server_ssl_context
from ext_proc_proxy.config import ProxyConfig

logger = logging.getLogger("ext_proc_proxy")

CONFIG_KEY = web.AppKey("config", ProxyConfig)
UPSTREAM_SSL_KEY = web.AppKey("upstream_ssl_context", ssl.SSLContext)
CLIENT_SESSION_KEY = web.AppKey("client_session", aiohttp.ClientSession)

# Standard Hop-by-Hop headers defined in RFC 2616 / RFC 7230 / RFC 9110
HOP_BY_HOP_HEADERS: Set[str] = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

# Headers that must not be propagated per proxy requirements
EXCLUDED_REQUEST_HEADERS: Set[str] = {
    "x-forwarded-for",
    "x-forwarded-host",
}


def filter_request_headers(headers: multidict.CIMultiDictProxy) -> multidict.CIMultiDict:
    """Filter hop-by-hop and excluded headers from incoming request headers."""
    filtered = multidict.CIMultiDict()
    connection_val = headers.get("connection", "")
    connection_tokens = {
        token.strip().lower() for token in connection_val.split(",") if token.strip()
    }
    strip_set = HOP_BY_HOP_HEADERS | connection_tokens | EXCLUDED_REQUEST_HEADERS

    for key, value in headers.items():
        if key.lower() not in strip_set:
            filtered.add(key, value)
    return filtered


def filter_response_headers(headers: multidict.CIMultiDictProxy) -> multidict.CIMultiDict:
    """Filter hop-by-hop headers from upstream response headers."""
    filtered = multidict.CIMultiDict()
    connection_val = headers.get("connection", "")
    connection_tokens = {
        token.strip().lower() for token in connection_val.split(",") if token.strip()
    }
    strip_set = HOP_BY_HOP_HEADERS | connection_tokens

    for key, value in headers.items():
        if key.lower() not in strip_set:
            filtered.add(key, value)
    return filtered


async def stream_request_payload(request: web.Request) -> AsyncIterator[bytes]:
    """Asynchronously stream the request body chunks."""
    async for chunk in request.content.iter_any():
        yield chunk


async def handle_proxy_request(request: web.Request) -> web.StreamResponse:
    """Handle incoming client HTTPS request and forward to target host."""
    # 1. Determine target scheme from X-Forwarded-Proto
    x_proto = request.headers.get("X-Forwarded-Proto")
    if x_proto is not None:
        proto_clean = x_proto.strip().lower()
        if proto_clean in ("http", "https"):
            target_scheme = proto_clean
        else:
            return web.Response(
                status=400,
                text=(
                    f"Invalid X-Forwarded-Proto header value '{x_proto}'. "
                    "Must be 'http' or 'https'."
                ),
            )
    else:
        # If the header is not present, assume HTTPS
        target_scheme = "https"

    # 2. Determine target host
    target_host = request.headers.get("Host") or request.host
    if not target_host:
        return web.Response(status=400, text="Missing Host header in request")

    # 3. Construct target URL
    # request.rel_url preserves path, query parameters, and fragments
    target_url = f"{target_scheme}://{target_host}{request.rel_url}"

    # 4. Prepare request headers and streaming body
    outgoing_headers = filter_request_headers(request.headers)

    body_data = None
    if request.can_read_body:
        body_data = stream_request_payload(request)

    session: aiohttp.ClientSession = request.app[CLIENT_SESSION_KEY]

    # 5. Forward request to target server
    try:
        upstream_cm = session.request(
            method=request.method,
            url=target_url,
            headers=outgoing_headers,
            data=body_data,
            allow_redirects=False,
        )
        upstream_resp = await upstream_cm.__aenter__()
    except (ssl.SSLCertVerificationError, aiohttp.ClientSSLError) as err:
        logger.warning("TLS certificate verification failed for %s: %s", target_url, err)
        return web.Response(
            status=502,
            text=f"502 Bad Gateway: Upstream TLS certificate verification failed ({err})",
        )
    except aiohttp.ClientConnectorError as err:
        logger.warning("Connection failed to %s: %s", target_url, err)
        return web.Response(
            status=502,
            text=f"502 Bad Gateway: Failed to connect to upstream ({err})",
        )
    except asyncio.TimeoutError:
        logger.warning("Request timed out to %s", target_url)
        return web.Response(
            status=504,
            text="504 Gateway Timeout: Upstream server timed out",
        )
    except aiohttp.ClientError as err:
        logger.warning("Upstream client error for %s: %s", target_url, err)
        return web.Response(
            status=502,
            text=f"502 Bad Gateway: Upstream client error ({err})",
        )

    # 6. Stream response back to original client connection
    try:
        resp_headers = filter_response_headers(upstream_resp.headers)
        response = web.StreamResponse(
            status=upstream_resp.status,
            reason=upstream_resp.reason,
            headers=resp_headers,
        )

        await response.prepare(request)

        # Do not attempt to read body for HEAD requests or body-less status codes
        if request.method != "HEAD" and upstream_resp.status not in (204, 304):
            async for chunk in upstream_resp.content.iter_any():
                await response.write(chunk)

        await response.write_eof()
        return response
    finally:
        await upstream_cm.__aexit__(None, None, None)


async def client_session_cleanup_ctx(app: web.Application):
    """Context manager for managing aiohttp.ClientSession lifecycle."""
    config: ProxyConfig = app[CONFIG_KEY]
    upstream_ssl_ctx = app.get(UPSTREAM_SSL_KEY)
    if upstream_ssl_ctx is None:
        # Default SSL context verifies target server TLS certificate
        upstream_ssl_ctx = ssl.create_default_context()

    connector = aiohttp.TCPConnector(ssl=upstream_ssl_ctx)
    timeout = ClientTimeout(total=config.upstream_timeout)
    session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    app[CLIENT_SESSION_KEY] = session

    yield

    await session.close()


def create_proxy_app(
    config: Optional[ProxyConfig] = None,
    upstream_ssl_context: Optional[ssl.SSLContext] = None,
) -> web.Application:
    """Create and configure the proxy web.Application."""
    if config is None:
        config = ProxyConfig()

    app = web.Application()
    app[CONFIG_KEY] = config
    if upstream_ssl_context is not None:
        app[UPSTREAM_SSL_KEY] = upstream_ssl_context

    app.cleanup_ctx.append(client_session_cleanup_ctx)

    # Catch-all route to handle all HTTP methods and paths
    app.router.add_route("*", "/{path_info:.*}", handle_proxy_request)

    return app


async def run_proxy(config: ProxyConfig) -> None:
    """Initialize and run the proxy server."""
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("Initializing server SSL context...")
    ssl_context = create_server_ssl_context(
        cert_file=config.cert,
        key_file=config.key,
        generate_self_signed=config.self_signed,
        hostname=config.host if config.host not in ("0.0.0.0", "::") else "localhost",
    )

    app = create_proxy_app(config)
    runner = web.AppRunner(app, keepalive_timeout=config.keepalive_timeout)
    await runner.setup()

    site = web.TCPSite(
        runner,
        host=config.host,
        port=config.port,
        ssl_context=ssl_context,
    )
    await site.start()
    logger.info(
        "Proxy server listening on https://%s:%d (keepalive: %.1fs)",
        config.host,
        config.port,
        config.keepalive_timeout,
    )

    try:
        while True:
            await asyncio.sleep(3600)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Shutting down proxy server...")
    finally:
        await runner.cleanup()
