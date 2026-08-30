"""Envoy External Processing (ext_proc) gRPC server implementation using grpc.aio."""

import asyncio
from dataclasses import dataclass
import logging
import ssl
from typing import AsyncIterator, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import aiohttp
import grpc
import multidict

from envoy.service.ext_proc.v3 import (
    external_processor_pb2,
    external_processor_pb2_grpc,
)
from ext_proc_proxy.config import ProxyConfig
from ext_proc_proxy.http_utils import (
    classify_upstream_error,
    create_client_session,
    filter_request_headers,
    filter_response_headers,
    is_bodyless_response,
)

logger = logging.getLogger("ext_proc_proxy.grpc")


@dataclass(frozen=True)
class RequestBodyChunk:
    """Request body chunk sent from Envoy to be streamed upstream."""

    data: bytes
    is_last: bool


@dataclass(frozen=True)
class UpstreamHeaders:
    """Upstream HTTP response status and filtered headers."""

    status: int
    headers: List[Tuple[str, str]]
    is_empty_body: bool


@dataclass(frozen=True)
class UpstreamBodyChunk:
    """Upstream HTTP response body chunk."""

    data: bytes
    is_last: bool


@dataclass(frozen=True)
class UpstreamError:
    """Upstream error with status code and descriptive message."""

    status: int
    message: str


UpstreamResponseItem = Union[UpstreamHeaders, UpstreamBodyChunk, UpstreamError]


class ExternalProcessorService(external_processor_pb2_grpc.ExternalProcessorServicer):
    """External Processor servicer handling bidirectional gRPC streams."""

    def __init__(
        self,
        config: ProxyConfig,
        session: Optional[aiohttp.ClientSession] = None,
        upstream_ssl_context: Optional[ssl.SSLContext] = None,
    ):
        self.config = config
        self._session = session
        self._owns_session = False
        self._upstream_ssl_context = upstream_ssl_context
        self.target_url_parsed = urlparse(config.ext_proc_target)

    async def get_session(self) -> aiohttp.ClientSession:
        """Get or initialize the aiohttp.ClientSession."""
        if self._session is None or self._session.closed:
            self._session = create_client_session(
                config=self.config,
                upstream_ssl_context=self._upstream_ssl_context,
            )
            self._owns_session = True
        return self._session

    async def close(self):
        """Close internal session if owned."""
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    async def _fetch_upstream_stream(
        self,
        session: aiohttp.ClientSession,
        method: str,
        target_url: str,
        headers: multidict.CIMultiDict,
        body_queue: Optional[asyncio.Queue[RequestBodyChunk]],
        resp_queue: asyncio.Queue[UpstreamResponseItem],
    ) -> None:
        """Execute upstream request and stream body/response via queues."""
        try:
            body_data = None
            if body_queue is not None:

                async def body_stream_generator():
                    while True:
                        chunk_item = await body_queue.get()
                        if chunk_item.data:
                            yield chunk_item.data
                        if chunk_item.is_last:
                            break

                body_data = body_stream_generator()

            async with session.request(
                method=method,
                url=target_url,
                headers=headers,
                data=body_data,
                allow_redirects=False,
            ) as upstream_resp:
                # Filter hop-by-hop headers from upstream response
                filtered_resp_headers = filter_response_headers(upstream_resp.headers)
                resp_headers = list(filtered_resp_headers.items())

                is_empty_body = is_bodyless_response(method, upstream_resp.status)

                await resp_queue.put(
                    UpstreamHeaders(
                        status=upstream_resp.status,
                        headers=resp_headers,
                        is_empty_body=is_empty_body,
                    )
                )

                if not is_empty_body:
                    async for chunk in upstream_resp.content.iter_any():
                        await resp_queue.put(UpstreamBodyChunk(data=chunk, is_last=False))

                # Signal end of response body
                await resp_queue.put(UpstreamBodyChunk(data=b"", is_last=True))

        except Exception as err:
            status_code, err_msg = classify_upstream_error(err)
            logger.warning("Upstream request error for %s: %s", target_url, err)
            await resp_queue.put(UpstreamError(status=status_code, message=err_msg))

    async def Process(
        self,
        request_iterator: AsyncIterator[external_processor_pb2.ProcessingRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[external_processor_pb2.ProcessingResponse]:
        """Handle bidirectional ext_proc stream from Envoy."""
        session = await self.get_session()
        upstream_task: Optional[asyncio.Task] = None

        try:
            # Read first request containing headers
            try:
                first_request = await anext(request_iterator)
            except StopAsyncIteration:
                return

            if first_request.observability_mode:
                # In observability mode, do not respond to messages
                async for _ in request_iterator:
                    pass
                return

            if not first_request.HasField("request_headers"):
                logger.warning("First request did not contain request_headers")
                return

            # Extract headers, method, path
            raw_headers: Dict[str, str] = {}
            for h in first_request.request_headers.headers.headers:
                raw_headers[h.key] = h.value or (
                    h.raw_value.decode("utf-8", "ignore") if h.raw_value else ""
                )

            method = raw_headers.get(":method", "GET").upper()
            path = raw_headers.get(":path", "/")
            if not path.startswith("/"):
                path = "/" + path

            # Construct target URL
            target_base = self.config.ext_proc_target.rstrip("/")
            target_url = f"{target_base}{path}"

            # Prepare outgoing headers
            outgoing_headers = filter_request_headers(raw_headers)
            # Set Host header to target netloc
            outgoing_headers["Host"] = self.target_url_parsed.netloc

            has_request_body = not first_request.request_headers.end_of_stream
            body_queue: Optional[asyncio.Queue[RequestBodyChunk]] = (
                asyncio.Queue() if has_request_body else None
            )
            resp_queue: asyncio.Queue[UpstreamResponseItem] = asyncio.Queue()

            upstream_task = asyncio.create_task(
                self._fetch_upstream_stream(
                    session=session,
                    method=method,
                    target_url=target_url,
                    headers=outgoing_headers,
                    body_queue=body_queue,
                    resp_queue=resp_queue,
                )
            )

            if has_request_body:
                # Acknowledge headers so Envoy begins sending request_body chunks
                continue_resp = external_processor_pb2.ProcessingResponse(
                    request_headers=external_processor_pb2.HeadersResponse(
                        response=external_processor_pb2.CommonResponse(
                            status=external_processor_pb2.CommonResponse.ResponseStatus.CONTINUE
                        )
                    )
                )
                yield continue_resp

                # Stream incoming body chunks from Envoy
                async for req in request_iterator:
                    if req.HasField("request_body"):
                        chunk = req.request_body.body
                        is_last = req.request_body.end_of_stream
                        await body_queue.put(RequestBodyChunk(data=chunk, is_last=is_last))

                        if not is_last:
                            # Acknowledge intermediate chunk
                            body_resp = external_processor_pb2.ProcessingResponse(
                                request_body=external_processor_pb2.BodyResponse(
                                    response=external_processor_pb2.CommonResponse(
                                        status=external_processor_pb2.CommonResponse.ResponseStatus.CONTINUE
                                    )
                                )
                            )
                            yield body_resp
                        else:
                            break

            # Now stream the upstream response back to Envoy
            while True:
                item = await resp_queue.get()

                if isinstance(item, UpstreamError):
                    err_bytes = item.message.encode("utf-8")
                    sir = external_processor_pb2.StreamedImmediateResponse()
                    # Headers
                    h_status = sir.headers_response.headers.headers.add()
                    h_status.key = ":status"
                    h_status.value = str(item.status)
                    h_ct = sir.headers_response.headers.headers.add()
                    h_ct.key = "content-type"
                    h_ct.value = "text/plain"
                    sir.headers_response.end_of_stream = False
                    yield external_processor_pb2.ProcessingResponse(
                        streamed_immediate_response=sir
                    )

                    # Body
                    sir_body = external_processor_pb2.StreamedImmediateResponse()
                    sir_body.body_response.body = err_bytes
                    sir_body.body_response.end_of_stream = True
                    yield external_processor_pb2.ProcessingResponse(
                        streamed_immediate_response=sir_body
                    )
                    break

                elif isinstance(item, UpstreamHeaders):
                    sir = external_processor_pb2.StreamedImmediateResponse()
                    h_status = sir.headers_response.headers.headers.add()
                    h_status.key = ":status"
                    h_status.value = str(item.status)

                    for k, v in item.headers:
                        hv = sir.headers_response.headers.headers.add()
                        hv.key = k.lower()
                        hv.value = str(v)

                    sir.headers_response.end_of_stream = item.is_empty_body
                    yield external_processor_pb2.ProcessingResponse(
                        streamed_immediate_response=sir
                    )
                    if item.is_empty_body:
                        break

                elif isinstance(item, UpstreamBodyChunk):
                    sir = external_processor_pb2.StreamedImmediateResponse()
                    sir.body_response.body = item.data
                    sir.body_response.end_of_stream = item.is_last
                    yield external_processor_pb2.ProcessingResponse(
                        streamed_immediate_response=sir
                    )
                    if item.is_last:
                        break

            await upstream_task

        except Exception as err:
            logger.exception("Error in ext_proc Process: %s", err)
            raise
        finally:
            if upstream_task is not None and not upstream_task.done():
                upstream_task.cancel()
                try:
                    await upstream_task
                except (asyncio.CancelledError, Exception):
                    pass


def create_ext_proc_server(
    config: ProxyConfig,
    server_credentials: grpc.ServerCredentials,
    session: Optional[aiohttp.ClientSession] = None,
    upstream_ssl_context: Optional[ssl.SSLContext] = None,
) -> grpc.aio.Server:
    """Create and configure the TLS-secured gRPC ext_proc server."""
    server = grpc.aio.server()
    service = ExternalProcessorService(
        config=config,
        session=session,
        upstream_ssl_context=upstream_ssl_context,
    )
    external_processor_pb2_grpc.add_ExternalProcessorServicer_to_server(service, server)
    listen_addr = f"{config.ext_proc_host}:{config.ext_proc_port}"
    server.add_secure_port(listen_addr, server_credentials)
    server.ext_proc_service = service  # type: ignore[attr-defined]
    return server


async def run_grpc_server(
    config: ProxyConfig,
    server_credentials: grpc.ServerCredentials,
    session: Optional[aiohttp.ClientSession] = None,
    upstream_ssl_context: Optional[ssl.SSLContext] = None,
) -> None:
    """Initialize and run the TLS-secured Envoy ext_proc gRPC server."""
    grpc_server = create_ext_proc_server(
        config=config,
        server_credentials=server_credentials,
        session=session,
        upstream_ssl_context=upstream_ssl_context,
    )
    await grpc_server.start()
    logger.info(
        "Envoy ext_proc gRPC server listening with TLS on %s:%d (target: %s)",
        config.ext_proc_host,
        config.ext_proc_port,
        config.ext_proc_target,
    )
    try:
        await grpc_server.wait_for_termination()
    finally:
        if hasattr(grpc_server, "ext_proc_service"):
            await grpc_server.ext_proc_service.close()
        await grpc_server.stop(grace=2.0)
