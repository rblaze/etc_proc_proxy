"""Session registry and IPC data structures for paired ext_proc and HTTP proxy requests."""

import asyncio
from dataclasses import dataclass, field
import logging
from typing import Dict, List, Optional, Tuple, Union

logger = logging.getLogger("ext_proc_proxy.session")


@dataclass(frozen=True)
class UpstreamRequestHeaders:
    """Request headers and metadata forwarded from HTTP proxy to Envoy."""

    method: str
    path: str
    headers: List[Tuple[str, str]]
    has_body: bool
    scheme: str


@dataclass(frozen=True)
class UpstreamRequestBodyChunk:
    """Request body chunk forwarded from HTTP proxy to Envoy."""

    data: bytes
    is_last: bool


@dataclass(frozen=True)
class EnvoyResponseHeaders:
    """Response headers received from Envoy forwarded to HTTP proxy."""

    status: int
    headers: List[Tuple[str, str]]
    is_empty_body: bool


@dataclass(frozen=True)
class EnvoyResponseBodyChunk:
    """Response body chunk received from Envoy forwarded to HTTP proxy."""

    data: bytes
    is_last: bool


@dataclass(frozen=True)
class SessionAbort:
    """Sentinel indicating connection cancellation or abort."""

    reason: str


RequestToEnvoyItem = Union[
    UpstreamRequestHeaders, UpstreamRequestBodyChunk, SessionAbort
]
ResponseFromEnvoyItem = Union[
    EnvoyResponseHeaders, EnvoyResponseBodyChunk, SessionAbort
]


@dataclass
class ExtProcSession:
    """State and queues for an active paired ext_proc session."""

    request_id: str
    request_to_envoy_queue: asyncio.Queue[RequestToEnvoyItem] = field(
        default_factory=asyncio.Queue
    )
    response_from_envoy_queue: asyncio.Queue[ResponseFromEnvoyItem] = field(
        default_factory=asyncio.Queue
    )
    is_paired: bool = False
    is_aborted: bool = False

    def abort(self, reason: str = "Session aborted") -> None:
        """Abort session and signal both queues."""
        if not self.is_aborted:
            self.is_aborted = True
            abort_msg = SessionAbort(reason=reason)
            self.request_to_envoy_queue.put_nowait(abort_msg)
            self.response_from_envoy_queue.put_nowait(abort_msg)


class SessionRegistry:
    """Thread-safe / asyncio registry of active ExtProcSession instances."""

    def __init__(self) -> None:
        self._sessions: Dict[str, ExtProcSession] = {}

    def register(self, session: ExtProcSession) -> None:
        """Register a new ExtProcSession."""
        self._sessions[session.request_id] = session
        logger.debug("Registered session for request ID %s", session.request_id)

    def get(self, request_id: str) -> Optional[ExtProcSession]:
        """Look up an active session by request ID."""
        return self._sessions.get(request_id)

    def unregister(self, request_id: str) -> Optional[ExtProcSession]:
        """Unregister a session when the ext_proc stream completes."""
        session = self._sessions.pop(request_id, None)
        if session:
            logger.debug("Unregistered session for request ID %s", request_id)
        return session

    def __len__(self) -> int:
        return len(self._sessions)

    def __bool__(self) -> bool:
        """Ensure SessionRegistry instances are always truthy regardless of item count."""
        return True
