"""Core proxy implementation using aiohttp."""

import asyncio
import logging
import ssl
from typing import AsyncIterator, Optional

import aiohttp
from aiohttp import web

from ext_proc_proxy.config import ProxyConfig
from ext_proc_proxy.http_utils import (
    classify_upstream_error,
    create_client_session,
    filter_request_headers,
    filter_response_headers,
    is_bodyless_response,
)

logger = logging.getLogger("http_proxy")

CONFIG_KEY = web.AppKey("config", ProxyConfig)
UPSTREAM_SSL_KEY = web.AppKey("upstream_ssl_context", ssl.SSLContext)
CLIENT_SESSION_KEY = web.AppKey("client_session", aiohttp.ClientSession)


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
    except Exception as err:
        status_code, err_msg = classify_upstream_error(err)
        logger.warning("Upstream request error for %s: %s", target_url, err)
        return web.Response(
            status=status_code,
            text=err_msg,
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
        if not is_bodyless_response(request.method, upstream_resp.status):
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
    session = create_client_session(config=config, upstream_ssl_context=upstream_ssl_ctx)
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


async def run_proxy(
    config: ProxyConfig,
    ssl_context: ssl.SSLContext,
) -> None:
    """Initialize and run the HTTPS proxy server."""
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
        "HTTPS proxy server listening on https://%s:%d (keepalive: %.1fs)",
        config.host,
        config.port,
        config.keepalive_timeout,
    )

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()
