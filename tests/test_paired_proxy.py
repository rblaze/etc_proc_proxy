"""Integration tests for paired ext_proc and HTTP proxy connection loop."""

import asyncio
import os
import ssl
from typing import List, Optional
import unittest

import aiohttp
from aiohttp import web
import grpc

from envoy.service.ext_proc.v3 import (
    external_processor_pb2,
    external_processor_pb2_grpc,
)
from ext_proc_proxy.cert_utils import (
    create_grpc_server_credentials,
    create_server_ssl_context,
    generate_self_signed_cert,
)
from ext_proc_proxy.config import ProxyConfig
from ext_proc_proxy.ext_proc_server import create_ext_proc_server
from ext_proc_proxy.http_utils import REQUEST_ID_HEADER
from ext_proc_proxy.proxy import create_proxy_app
from ext_proc_proxy.session_registry import SessionRegistry


class TestPairedProxy(unittest.IsolatedAsyncioTestCase):
    """Integration test suite for the paired ext_proc & HTTP proxy workflow."""

    @classmethod
    def setUpClass(cls):
        # Generate TLS certificates for servers
        cls.cert_file, cls.key_file = generate_self_signed_cert(hostname="localhost")
        with open(cls.cert_file, "rb") as f:
            cls.cert_bytes = f.read()

        cls.server_ssl_ctx = create_server_ssl_context(
            cert_file=cls.cert_file, key_file=cls.key_file
        )
        cls.client_ssl_ctx = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH, cafile=cls.cert_file
        )

    @classmethod
    def tearDownClass(cls):
        for f in (cls.cert_file, cls.key_file):
            if os.path.exists(f):
                os.remove(f)

    async def asyncSetUp(self):
        self.http_runners: List[web.AppRunner] = []
        self.grpc_servers: List[grpc.aio.Server] = []
        self.session_registry = SessionRegistry()

    async def asyncTearDown(self):
        for server in reversed(self.grpc_servers):
            if hasattr(server, "ext_proc_service"):
                await server.ext_proc_service.close()
            await server.stop(grace=1.0)
        for runner in reversed(self.http_runners):
            await runner.cleanup()

    async def _start_http_server(
        self, handler, ssl_context: Optional[ssl.SSLContext] = None
    ) -> int:
        """Start an aiohttp HTTP/HTTPS server and return its port."""
        app = web.Application()
        app.router.add_route("*", "/{path_info:.*}", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        self.http_runners.append(runner)

        site = web.TCPSite(runner, host="127.0.0.1", port=0, ssl_context=ssl_context)
        await site.start()
        return runner.addresses[0][1]

    async def _start_proxy_server(self) -> int:
        """Start the proxy server with shared session_registry and return port."""
        config = ProxyConfig(
            host="127.0.0.1",
            port=0,
            cert=self.cert_file,
            key=self.key_file,
        )
        app = create_proxy_app(
            config=config,
            session_registry=self.session_registry,
        )
        runner = web.AppRunner(app, keepalive_timeout=75.0)
        await runner.setup()
        self.http_runners.append(runner)

        site = web.TCPSite(
            runner,
            host="127.0.0.1",
            port=0,
            ssl_context=self.server_ssl_ctx,
        )
        await site.start()
        return runner.addresses[0][1]

    async def _start_ext_proc_server(
        self,
        upstream_port: int,
        upstream_timeout: float = 5.0,
    ) -> int:
        """Start the ext_proc gRPC server targeting the upstream service port."""
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            grpc_port = s.getsockname()[1]

        config = ProxyConfig(
            ext_proc_host="127.0.0.1",
            ext_proc_port=grpc_port,
            ext_proc_target=f"http://127.0.0.1:{upstream_port}",
            cert=self.cert_file,
            key=self.key_file,
            upstream_timeout=upstream_timeout,
        )

        creds = create_grpc_server_credentials(self.cert_file, self.key_file)
        grpc_server = create_ext_proc_server(
            config=config,
            server_credentials=creds,
            session_registry=self.session_registry,
        )
        await grpc_server.start()
        self.grpc_servers.append(grpc_server)
        return grpc_port

    def _get_client_credentials(self) -> grpc.ChannelCredentials:
        """Get gRPC channel credentials trusting server certificate."""
        return grpc.ssl_channel_credentials(root_certificates=self.cert_bytes)

    async def test_full_paired_roundtrip_loop(self):
        """Test complete paired loop: Envoy -> gRPC -> Upstream -> Proxy -> Envoy Target -> Proxy -> Upstream -> gRPC -> Envoy."""
        proxy_port = await self._start_proxy_server()

        received_by_upstream_service = {}
        target_received_by_upstream = {}

        # Upstream Service mock handler:
        # Receives call from gRPC server, extracts request ID, calls HTTP proxy, gets target response, returns final payload
        async def mock_upstream_handler(request: web.Request):
            req_id = request.headers.get(REQUEST_ID_HEADER)
            body = await request.text()
            received_by_upstream_service["request_id"] = req_id
            received_by_upstream_service["path"] = request.path
            received_by_upstream_service["body"] = body

            # Call HTTP proxy with X-Ai-Proxy-Request-Id
            connector = aiohttp.TCPConnector(ssl=self.client_ssl_ctx)
            async with aiohttp.ClientSession(connector=connector) as client:
                proxy_url = f"https://127.0.0.1:{proxy_port}/transformed/path"
                headers = {
                    REQUEST_ID_HEADER: req_id,
                    "X-Transformed-Header": "AiEnriched",
                }
                async with client.post(
                    proxy_url, headers=headers, data="transformed request body"
                ) as proxy_resp:
                    target_received_by_upstream["status"] = proxy_resp.status
                    target_received_by_upstream["custom_hdr"] = proxy_resp.headers.get(
                        "X-Target-Custom"
                    )
                    target_received_by_upstream["body"] = await proxy_resp.text()

            # Final response from Upstream Service returned to gRPC server
            resp = web.Response(
                text="final postprocessed response from upstream service",
                status=200,
            )
            resp.headers["X-Final-Service-Header"] = "FinalVal"
            return resp

        upstream_port = await self._start_http_server(mock_upstream_handler)
        grpc_port = await self._start_ext_proc_server(upstream_port)

        creds = self._get_client_credentials()
        async with grpc.aio.secure_channel(f"localhost:{grpc_port}", creds) as channel:
            stub = external_processor_pb2_grpc.ExternalProcessorStub(channel)

            input_queue: asyncio.Queue = asyncio.Queue()

            async def request_generator():
                while True:
                    item = await input_queue.get()
                    if item is None:
                        break
                    yield item

            call = stub.Process(request_generator())

            # 1. Send initial request from Envoy to gRPC server (POST with body)
            req1 = external_processor_pb2.ProcessingRequest()
            h1 = req1.request_headers.headers.headers.add()
            h1.key = ":method"
            h1.value = "POST"
            h2 = req1.request_headers.headers.headers.add()
            h2.key = ":path"
            h2.value = "/initial/path"
            req1.request_headers.end_of_stream = False
            await input_queue.put(req1)

            # 2. Stream initial request body chunk from Envoy (no CONTINUE confirmation needed in FULL_DUPLEX_STREAMED mode)
            req_chunk = external_processor_pb2.ProcessingRequest()
            req_chunk.request_body.body = b"initial prompt payload"
            req_chunk.request_body.end_of_stream = True
            await input_queue.put(req_chunk)

            # 3. Now Upstream Service has called HTTP Proxy.
            # gRPC server sends ProcessingResponse with HeaderMutation to Envoy!
            proxy_req_headers = await call.read()
            self.assertTrue(proxy_req_headers.HasField("request_headers"))
            hdr_mut = (
                proxy_req_headers.request_headers.response.header_mutation.set_headers
            )
            mutated_headers = {
                h.header.key: (h.header.raw_value.decode("utf-8") if h.header.raw_value else h.header.value)
                for h in hdr_mut
            }
            self.assertEqual(
                mutated_headers.get("x-transformed-header"), "AiEnriched"
            )
            # Ensure internal request ID was stripped from outgoing request to Target
            self.assertNotIn(REQUEST_ID_HEADER, mutated_headers)

            # 5. gRPC server sends BodyMutation for the proxy request body
            proxy_req_body = await call.read()
            self.assertTrue(proxy_req_body.HasField("request_body"))
            body_mut = proxy_req_body.request_body.response.body_mutation
            self.assertTrue(body_mut.HasField("streamed_response"))
            self.assertEqual(body_mut.streamed_response.body, b"transformed request body")
            self.assertTrue(body_mut.streamed_response.end_of_stream)

            # 6. Envoy forwards to Target, Target replies.
            # Envoy sends ProcessingRequest(response_headers) & ProcessingRequest(response_body) on gRPC stream
            target_resp_hdr = external_processor_pb2.ProcessingRequest()
            th1 = target_resp_hdr.response_headers.headers.headers.add()
            th1.key = ":status"
            th1.value = "200"
            th2 = target_resp_hdr.response_headers.headers.headers.add()
            th2.key = "x-target-custom"
            th2.value = "TargetResponseHeaderVal"
            target_resp_hdr.response_headers.end_of_stream = False
            await input_queue.put(target_resp_hdr)

            target_resp_body = external_processor_pb2.ProcessingRequest()
            target_resp_body.response_body.body = b"target server payload data"
            target_resp_body.response_body.end_of_stream = True
            await input_queue.put(target_resp_body)

            # Close generator input
            await input_queue.put(None)

            # 7. gRPC server sends final response (HeaderMutation on response_headers & BodyMutation on response_body)
            final_resp_hdrs = await call.read()
            self.assertTrue(final_resp_hdrs.HasField("response_headers"))
            final_hdr_mut = (
                final_resp_hdrs.response_headers.response.header_mutation.set_headers
            )
            final_headers_dict = {
                h.header.key: (h.header.raw_value.decode("utf-8") if h.header.raw_value else h.header.value)
                for h in final_hdr_mut
            }
            self.assertEqual(final_headers_dict.get(":status"), "200")
            self.assertEqual(
                final_headers_dict.get("x-final-service-header"), "FinalVal"
            )

            final_resp_body = await call.read()
            self.assertTrue(final_resp_body.HasField("response_body"))
            final_body_mut = final_resp_body.response_body.response.body_mutation
            self.assertTrue(final_body_mut.HasField("streamed_response"))
            self.assertEqual(
                final_body_mut.streamed_response.body,
                b"final postprocessed response from upstream service",
            )
            self.assertTrue(final_body_mut.streamed_response.end_of_stream)

            # Verify Upstream Service received all intermediate data
            self.assertIn("request_id", received_by_upstream_service)
            self.assertEqual(
                received_by_upstream_service["body"], "initial prompt payload"
            )
            self.assertEqual(target_received_by_upstream["status"], 200)
            self.assertEqual(
                target_received_by_upstream["custom_hdr"],
                "TargetResponseHeaderVal",
            )
            self.assertEqual(
                target_received_by_upstream["body"],
                "target server payload data",
            )

    async def test_invalid_or_expired_request_id_returns_400(self):
        """Test that HTTP proxy returns 400 Bad Request when X-Ai-Proxy-Request-Id is invalid."""
        proxy_port = await self._start_proxy_server()

        connector = aiohttp.TCPConnector(ssl=self.client_ssl_ctx)
        async with aiohttp.ClientSession(connector=connector) as client:
            url = f"https://127.0.0.1:{proxy_port}/api/endpoint"
            headers = {
                REQUEST_ID_HEADER: "nonexistent-or-expired-request-id-12345",
            }
            async with client.post(url, headers=headers, data="data") as resp:
                self.assertEqual(resp.status, 400)
                text = await resp.text()
                self.assertIn("Invalid or expired", text)
                self.assertIn("nonexistent-or-expired-request-id-12345", text)

    async def test_unpaired_short_circuit_immediate_response(self):
        """Test Path B: Upstream service immediately returns 403 Forbidden without calling HTTP proxy."""
        async def mock_short_circuit_handler(request: web.Request):
            return web.Response(
                text="Access Denied by Policy",
                status=403,
                headers={"X-Blocked-By": "Guardrail"},
            )

        upstream_port = await self._start_http_server(mock_short_circuit_handler)
        grpc_port = await self._start_ext_proc_server(upstream_port)

        creds = self._get_client_credentials()
        async with grpc.aio.secure_channel(f"localhost:{grpc_port}", creds) as channel:
            stub = external_processor_pb2_grpc.ExternalProcessorStub(channel)

            async def request_generator():
                req = external_processor_pb2.ProcessingRequest()
                h1 = req.request_headers.headers.headers.add()
                h1.key = ":method"
                h1.value = "GET"
                h2 = req.request_headers.headers.headers.add()
                h2.key = ":path"
                h2.value = "/blocked-path"
                req.request_headers.end_of_stream = True
                yield req

            responses = []
            async for resp in stub.Process(request_generator()):
                responses.append(resp)

            self.assertGreaterEqual(len(responses), 2)
            first_resp = responses[0]
            self.assertTrue(first_resp.HasField("streamed_immediate_response"))
            sir_hdrs = first_resp.streamed_immediate_response.headers_response.headers.headers
            hdrs_dict = {
                h.key: (h.raw_value.decode("utf-8") if h.raw_value else h.value)
                for h in sir_hdrs
            }
            self.assertEqual(hdrs_dict.get(":status"), "403")
            self.assertEqual(hdrs_dict.get("x-blocked-by"), "Guardrail")

            body_chunks = [
                r.streamed_immediate_response.body_response.body
                for r in responses[1:]
                if r.streamed_immediate_response.HasField("body_response")
            ]
            self.assertEqual(b"".join(body_chunks), b"Access Denied by Policy")

    async def test_envoy_abort_propagates_to_paired_http_connection(self):
        """Test that if Envoy aborts the gRPC connection, the paired HTTP proxy connection aborts."""
        proxy_port = await self._start_proxy_server()

        http_proxy_error_caught = asyncio.Event()

        async def mock_upstream_handler(request: web.Request):
            req_id = request.headers.get(REQUEST_ID_HEADER)
            connector = aiohttp.TCPConnector(ssl=self.client_ssl_ctx)
            async with aiohttp.ClientSession(connector=connector) as client:
                proxy_url = f"https://127.0.0.1:{proxy_port}/wait-target"
                headers = {REQUEST_ID_HEADER: req_id}
                try:
                    async with client.get(proxy_url, headers=headers) as resp:
                        # Wait for response body which will never arrive because Envoy aborts
                        await resp.read()
                except (aiohttp.ClientError, Exception):
                    http_proxy_error_caught.set()
                else:
                    if resp.status >= 500:
                        http_proxy_error_caught.set()

            return web.Response(text="done", status=200)

        upstream_port = await self._start_http_server(mock_upstream_handler)
        grpc_port = await self._start_ext_proc_server(upstream_port)

        creds = self._get_client_credentials()
        async with grpc.aio.secure_channel(f"localhost:{grpc_port}", creds) as channel:
            stub = external_processor_pb2_grpc.ExternalProcessorStub(channel)

            input_queue: asyncio.Queue = asyncio.Queue()

            async def request_generator():
                while True:
                    item = await input_queue.get()
                    if item is None:
                        break
                    yield item

            call = stub.Process(request_generator())

            # 1. Send initial request
            req1 = external_processor_pb2.ProcessingRequest()
            h1 = req1.request_headers.headers.headers.add()
            h1.key = ":method"
            h1.value = "GET"
            h2 = req1.request_headers.headers.headers.add()
            h2.key = ":path"
            h2.value = "/wait"
            req1.request_headers.end_of_stream = True
            await input_queue.put(req1)

            # 2. Wait for gRPC server to send request_headers mutation
            proxy_req_headers = await call.read()
            self.assertTrue(proxy_req_headers.HasField("request_headers"))

            # 3. Now Envoy cancels / closes the gRPC stream
            call.cancel()

            # 4. Verify HTTP proxy connection caught error/abort within 2 seconds
            try:
                await asyncio.wait_for(http_proxy_error_caught.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                self.fail("HTTP proxy connection did not abort when Envoy cancelled gRPC stream")

    async def test_paired_get_request_without_body(self):
        """Test paired mode when Upstream Service makes a GET request without body to HTTP proxy."""
        proxy_port = await self._start_proxy_server()

        async def mock_upstream_handler(request: web.Request):
            req_id = request.headers.get(REQUEST_ID_HEADER)
            connector = aiohttp.TCPConnector(ssl=self.client_ssl_ctx)
            async with aiohttp.ClientSession(connector=connector) as client:
                proxy_url = f"https://127.0.0.1:{proxy_port}/api/get-info?id=42"
                headers = {
                    REQUEST_ID_HEADER: req_id,
                    "X-Query-Filter": "active",
                }
                async with client.get(proxy_url, headers=headers) as proxy_resp:
                    target_body = await proxy_resp.text()

            resp = web.Response(text=f"upstream processed: {target_body}", status=200)
            resp.headers["X-Upstream-Result"] = "Success"
            return resp

        upstream_port = await self._start_http_server(mock_upstream_handler)
        grpc_port = await self._start_ext_proc_server(upstream_port)

        creds = self._get_client_credentials()
        async with grpc.aio.secure_channel(f"localhost:{grpc_port}", creds) as channel:
            stub = external_processor_pb2_grpc.ExternalProcessorStub(channel)

            input_queue: asyncio.Queue = asyncio.Queue()

            async def request_generator():
                while True:
                    item = await input_queue.get()
                    if item is None:
                        break
                    yield item

            call = stub.Process(request_generator())

            # 1. Send initial GET from Envoy
            req1 = external_processor_pb2.ProcessingRequest()
            h1 = req1.request_headers.headers.headers.add()
            h1.key = ":method"
            h1.value = "GET"
            h2 = req1.request_headers.headers.headers.add()
            h2.key = ":path"
            h2.value = "/api/get-info"
            req1.request_headers.end_of_stream = True
            await input_queue.put(req1)

            # 2. Receive request_headers mutation from Upstream Service via HTTP proxy
            proxy_req_headers = await call.read()
            self.assertTrue(proxy_req_headers.HasField("request_headers"))
            mutated_hdrs = {
                h.header.key: (h.header.raw_value.decode("utf-8") if h.header.raw_value else h.header.value)
                for h in proxy_req_headers.request_headers.response.header_mutation.set_headers
            }
            self.assertEqual(mutated_hdrs.get("x-query-filter"), "active")
            self.assertEqual(mutated_hdrs.get(":method"), "GET")
            self.assertEqual(mutated_hdrs.get(":path"), "/api/get-info?id=42")
            self.assertEqual(mutated_hdrs.get(":scheme"), "https")
            self.assertNotIn(REQUEST_ID_HEADER, mutated_hdrs)

            # 3. Envoy sends Target's response
            target_resp = external_processor_pb2.ProcessingRequest()
            th1 = target_resp.response_headers.headers.headers.add()
            th1.key = ":status"
            th1.value = "200"
            target_resp.response_headers.end_of_stream = False
            await input_queue.put(target_resp)

            target_body = external_processor_pb2.ProcessingRequest()
            target_body.response_body.body = b"data from target database"
            target_body.response_body.end_of_stream = True
            await input_queue.put(target_body)

            await input_queue.put(None)

            # 4. gRPC server returns final response
            final_resp_hdrs = await call.read()
            self.assertTrue(final_resp_hdrs.HasField("response_headers"))
            final_hdr_mut = {
                h.header.key: (h.header.raw_value.decode("utf-8") if h.header.raw_value else h.header.value)
                for h in final_resp_hdrs.response_headers.response.header_mutation.set_headers
            }
            self.assertEqual(final_hdr_mut.get(":status"), "200")
            self.assertEqual(final_hdr_mut.get("x-upstream-result"), "Success")

            final_resp_body = await call.read()
            self.assertTrue(final_resp_body.HasField("response_body"))
            final_body_mut = final_resp_body.response_body.response.body_mutation
            self.assertTrue(final_body_mut.HasField("streamed_response"))
            self.assertEqual(
                final_body_mut.streamed_response.body,
                b"upstream processed: data from target database",
            )
            self.assertTrue(final_body_mut.streamed_response.end_of_stream)

    async def test_paired_proxy_request_with_x_forwarded_proto_http(self):
        """Test that X-Forwarded-Proto: http in paired HTTP proxy request sets :scheme to http in HeaderMutation."""
        proxy_port = await self._start_proxy_server()

        async def mock_upstream_handler(request: web.Request):
            req_id = request.headers.get(REQUEST_ID_HEADER)
            connector = aiohttp.TCPConnector(ssl=self.client_ssl_ctx)
            async with aiohttp.ClientSession(connector=connector) as client:
                proxy_url = f"https://127.0.0.1:{proxy_port}/api/http-target"
                headers = {
                    REQUEST_ID_HEADER: req_id,
                    "X-Forwarded-Proto": "http",
                }
                async with client.post(proxy_url, headers=headers, data=b"ping") as proxy_resp:
                    target_body = await proxy_resp.text()

            return web.Response(text=f"got: {target_body}", status=200)

        upstream_port = await self._start_http_server(mock_upstream_handler)
        grpc_port = await self._start_ext_proc_server(upstream_port)

        creds = self._get_client_credentials()
        async with grpc.aio.secure_channel(f"localhost:{grpc_port}", creds) as channel:
            stub = external_processor_pb2_grpc.ExternalProcessorStub(channel)

            input_queue: asyncio.Queue = asyncio.Queue()

            async def request_generator():
                while True:
                    item = await input_queue.get()
                    if item is None:
                        break
                    yield item

            call = stub.Process(request_generator())

            # 1. Send initial request from Envoy
            req1 = external_processor_pb2.ProcessingRequest()
            h1 = req1.request_headers.headers.headers.add()
            h1.key = ":method"
            h1.value = "GET"
            h2 = req1.request_headers.headers.headers.add()
            h2.key = ":path"
            h2.value = "/initial"
            req1.request_headers.end_of_stream = True
            await input_queue.put(req1)

            # 2. Receive request_headers mutation from Upstream Service via HTTP proxy
            proxy_req_headers = await call.read()
            self.assertTrue(proxy_req_headers.HasField("request_headers"))
            mutated_hdrs = {
                h.header.key: (h.header.raw_value.decode("utf-8") if h.header.raw_value else h.header.value)
                for h in proxy_req_headers.request_headers.response.header_mutation.set_headers
            }
            self.assertEqual(mutated_hdrs.get(":method"), "POST")
            self.assertEqual(mutated_hdrs.get(":path"), "/api/http-target")
            self.assertEqual(mutated_hdrs.get(":scheme"), "http")
            self.assertNotIn(REQUEST_ID_HEADER, mutated_hdrs)
            self.assertNotIn("x-forwarded-proto", mutated_hdrs)

            # Read body mutation
            proxy_req_body = await call.read()
            self.assertTrue(proxy_req_body.HasField("request_body"))
            body_mut = proxy_req_body.request_body.response.body_mutation
            self.assertTrue(body_mut.HasField("streamed_response"))
            self.assertEqual(body_mut.streamed_response.body, b"ping")
            self.assertTrue(body_mut.streamed_response.end_of_stream)

            # 3. Envoy sends Target's response
            target_resp = external_processor_pb2.ProcessingRequest()
            th1 = target_resp.response_headers.headers.headers.add()
            th1.key = ":status"
            th1.value = "200"
            target_resp.response_headers.end_of_stream = False
            await input_queue.put(target_resp)

            target_body = external_processor_pb2.ProcessingRequest()
            target_body.response_body.body = b"pong"
            target_body.response_body.end_of_stream = True
            await input_queue.put(target_body)
            await input_queue.put(None)

            # 4. gRPC server returns final response
            final_resp_hdrs = await call.read()
            self.assertTrue(final_resp_hdrs.HasField("response_headers"))
            final_resp_body = await call.read()
            self.assertTrue(final_resp_body.HasField("response_body"))
            final_body_mut = final_resp_body.response_body.response.body_mutation
            self.assertTrue(final_body_mut.HasField("streamed_response"))
            self.assertEqual(
                final_body_mut.streamed_response.body,
                b"got: pong",
            )
            self.assertTrue(final_body_mut.streamed_response.end_of_stream)

    async def test_paired_proxy_request_with_invalid_x_forwarded_proto(self):
        """Test that invalid X-Forwarded-Proto in paired HTTP proxy request returns 400 Bad Request."""
        proxy_port = await self._start_proxy_server()

        upstream_received_status = None

        async def mock_upstream_handler(request: web.Request):
            nonlocal upstream_received_status
            req_id = request.headers.get(REQUEST_ID_HEADER)
            connector = aiohttp.TCPConnector(ssl=self.client_ssl_ctx)
            async with aiohttp.ClientSession(connector=connector) as client:
                proxy_url = f"https://127.0.0.1:{proxy_port}/api/test"
                headers = {
                    REQUEST_ID_HEADER: req_id,
                    "X-Forwarded-Proto": "ftp",
                }
                async with client.get(proxy_url, headers=headers) as proxy_resp:
                    upstream_received_status = proxy_resp.status
                    text = await proxy_resp.text()

            return web.Response(text=text, status=upstream_received_status)

        upstream_port = await self._start_http_server(mock_upstream_handler)
        grpc_port = await self._start_ext_proc_server(upstream_port)

        creds = self._get_client_credentials()
        async with grpc.aio.secure_channel(f"localhost:{grpc_port}", creds) as channel:
            stub = external_processor_pb2_grpc.ExternalProcessorStub(channel)

            input_queue: asyncio.Queue = asyncio.Queue()

            async def request_generator():
                while True:
                    item = await input_queue.get()
                    if item is None:
                        break
                    yield item

            call = stub.Process(request_generator())

            # 1. Send initial request from Envoy
            req1 = external_processor_pb2.ProcessingRequest()
            h1 = req1.request_headers.headers.headers.add()
            h1.key = ":method"
            h1.value = "GET"
            h2 = req1.request_headers.headers.headers.add()
            h2.key = ":path"
            h2.value = "/initial"
            req1.request_headers.end_of_stream = True
            await input_queue.put(req1)
            await input_queue.put(None)

            # 2. Upstream receives 400 from HTTP Proxy and returns direct response to gRPC
            resp = await call.read()
            self.assertTrue(resp.HasField("streamed_immediate_response"))
            sir_hdrs = {
                h.key: (h.raw_value.decode("utf-8") if h.raw_value else h.value)
                for h in resp.streamed_immediate_response.headers_response.headers.headers
            }
            self.assertEqual(sir_hdrs.get(":status"), "400")
            self.assertEqual(upstream_received_status, 400)


if __name__ == "__main__":
    unittest.main()
