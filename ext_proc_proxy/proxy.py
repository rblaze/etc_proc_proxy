"""Core proxy implementation using aiohttp."""

import asyncio
import logging
import ssl
from typing import AsyncIterator, Optional

import aiohttp
from aiohttp import web
import multidict

from ext_proc_proxy.config import ProxyConfig
from ext_proc_proxy.http_utils import (
    REQUEST_ID_HEADER,
    classify_upstream_error,
    create_client_session,
    filter_request_headers,
    filter_response_headers,
    is_bodyless_response,
)
from ext_proc_proxy.session_registry import (
    EnvoyResponseBodyChunk,
    EnvoyResponseHeaders,
    ExtProcSession,
    SessionAbort,
    SessionRegistry,
    UpstreamRequestBodyChunk,
    UpstreamRequestHeaders,
)

logger = logging.getLogger("ext_proc_proxy.http")

CONFIG_KEY = web.AppKey("config", ProxyConfig)
UPSTREAM_SSL_KEY = web.AppKey("upstream_ssl_context", ssl.SSLContext)
CLIENT_SESSION_KEY = web.AppKey("client_session", aiohttp.ClientSession)
SESSION_REGISTRY_KEY = web.AppKey("session_registry", SessionRegistry)


async def stream_request_payload(request: web.Request) -> AsyncIterator[bytes]:
    """Asynchronously stream the request body chunks."""
    async for chunk in request.content.iter_any():
        yield chunk


def _extract_scheme(request: web.Request) -> Tuple[Optional[str], Optional[web.Response]]:
    """Extract and validate target scheme from X-Forwarded-Proto header."""
    x_proto = request.headers.get("X-Forwarded-Proto")
    if x_proto is not None:
        proto_clean = x_proto.strip().lower()
        if proto_clean in ("http", "https"):
            return proto_clean, None
        return None, web.Response(
            status=400,
            text=(
                f"Invalid X-Forwarded-Proto header value '{x_proto}'. "
                "Must be 'http' or 'https'."
            ),
        )
    return "https", None


async def handle_paired_proxy_request(
    request: web.Request,
    session: ExtProcSession,
) -> web.StreamResponse:
    """Handle request paired with an ext_proc session via X-Ai-Proxy-Request-Id."""
    logger.info("gRPC proxy request: %s %s", request.method, request.url)
    session.is_paired = True
    target_scheme, err_response = _extract_scheme(request)
    if err_response is not None:
        return err_response

    outgoing_headers = filter_request_headers(request.headers)
    req_headers_list = list(outgoing_headers.items())
    prepared = False

    try:
        # 1. Feed request headers to the paired gRPC session queue
        await session.request_to_envoy_queue.put(
            UpstreamRequestHeaders(
                method=request.method,
                path=str(request.rel_url),
                headers=req_headers_list,
                has_body=request.can_read_body,
                scheme=target_scheme,
            )
        )

        # 2. Stream request body chunks if present
        if request.can_read_body:
            content_iter = request.content.iter_any()
            try:
                prev_chunk = await anext(content_iter)
            except StopAsyncIteration:
                await session.request_to_envoy_queue.put(
                    UpstreamRequestBodyChunk(data=b"", is_last=True)
                )
            else:
                async for next_chunk in content_iter:
                    await session.request_to_envoy_queue.put(
                        UpstreamRequestBodyChunk(data=prev_chunk, is_last=False)
                    )
                    prev_chunk = next_chunk
                await session.request_to_envoy_queue.put(
                    UpstreamRequestBodyChunk(data=prev_chunk, is_last=True)
                )

        # TODO: it seems that this code will not react to the SessionAbort
        # from Envoy until the full request is read from the HTTP connection
        # and enqueued for sending. It will not process any response messages
        # either, for example if the final target server returns an error
        # without waiting for the full request body.
        # It's arguably okay for MVP, but may need to be addressed later. My
        # concern is that requests can be quite big if they include a lot of
        # context and keeping them in memory in case of SessionAbort will cause
        # unnecessary waste until the full request is read.

        # 3. Read response from Envoy via the paired session response queue
        first_item = await session.response_from_envoy_queue.get()
        if isinstance(first_item, SessionAbort):
            return web.Response(
                status=502,
                text=f"502 Bad Gateway: {first_item.reason}",
            )
        if not isinstance(first_item, EnvoyResponseHeaders):
            return web.Response(
                status=502,
                text="502 Bad Gateway: Unexpected response from ext_proc session",
            )

        resp_headers = multidict.CIMultiDict(first_item.headers)
        response = web.StreamResponse(
            status=first_item.status,
            headers=resp_headers,
        )
        await response.prepare(request)
        prepared = True

        # 4. Stream response body chunks if not a body-less response
        if not first_item.is_empty_body:
            while True:
                item = await session.response_from_envoy_queue.get()
                if isinstance(item, SessionAbort):
                    break
                if isinstance(item, EnvoyResponseBodyChunk):
                    if item.data:
                        await response.write(item.data)
                    if item.is_last:
                        break

        await response.write_eof()
        return response

    except Exception as err:
        logger.warning(
            "Error in paired proxy request for %s: %s", session.request_id, err
        )
        session.abort(f"HTTP proxy error: {err}")
        if not prepared:
            status_code, err_msg = classify_upstream_error(err)
            return web.Response(status=status_code, text=err_msg)
        raise


async def handle_proxy_request(request: web.Request) -> web.StreamResponse:
    """Handle incoming client HTTPS request and forward to target host."""
    # Check if request has X-Ai-Proxy-Request-Id header for paired session routing
    request_id = request.headers.get(REQUEST_ID_HEADER)
    if request_id is not None:
        session_registry: Optional[SessionRegistry] = request.app.get(
            SESSION_REGISTRY_KEY
        )
        session = (
            session_registry.get(request_id)
            if session_registry is not None
            else None
        )
        if session is None:
            return web.Response(
                status=400,
                text=(
                    f"400 Bad Request: Invalid or expired {REQUEST_ID_HEADER} "
                    f"'{request_id}'"
                ),
            )
        return await handle_paired_proxy_request(request, session)

    logger.info("Direct request: %s %s", request.method, request.url)
    # Standard proxying behavior when no X-Ai-Proxy-Request-Id is present:
    # 1. Determine target scheme from X-Forwarded-Proto
    target_scheme, err_response = _extract_scheme(request)
    if err_response is not None:
        return err_response

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
    session = create_client_session(
        config=config, upstream_ssl_context=upstream_ssl_ctx
    )
    app[CLIENT_SESSION_KEY] = session

    yield

    await session.close()


def create_proxy_app(
    config: Optional[ProxyConfig] = None,
    upstream_ssl_context: Optional[ssl.SSLContext] = None,
    session_registry: Optional[SessionRegistry] = None,
) -> web.Application:
    """Create and configure the proxy web.Application."""
    if config is None:
        config = ProxyConfig()

    app = web.Application()
    app[CONFIG_KEY] = config
    if upstream_ssl_context is not None:
        app[UPSTREAM_SSL_KEY] = upstream_ssl_context
    if session_registry is not None:
        app[SESSION_REGISTRY_KEY] = session_registry

    app.cleanup_ctx.append(client_session_cleanup_ctx)

    # Catch-all route to handle all HTTP methods and paths
    app.router.add_route("*", "/{path_info:.*}", handle_proxy_request)

    return app


async def run_proxy(
    config: ProxyConfig,
    ssl_context: ssl.SSLContext,
    session_registry: SessionRegistry,
) -> None:
    """Initialize and run the HTTPS proxy server."""
    app = create_proxy_app(config=config, session_registry=session_registry)
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
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
