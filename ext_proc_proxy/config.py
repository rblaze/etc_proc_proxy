"""Configuration and argument parsing for ext_proc_proxy."""

import argparse
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class ProxyConfig:
    """Proxy server configuration."""

    host: str = "0.0.0.0"
    port: int = 8443
    cert: Optional[str] = None
    key: Optional[str] = None
    self_signed: bool = False
    keepalive_timeout: float = 75.0
    upstream_timeout: float = 60.0
    log_level: str = "INFO"


def parse_args(args: Optional[List[str]] = None) -> ProxyConfig:
    """Parse command line arguments into ProxyConfig."""
    parser = argparse.ArgumentParser(
        description="HTTPS-terminating HTTP/HTTPS proxy using aiohttp",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host/IP address to bind the proxy server to",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=8443,
        help="Port to listen for incoming HTTPS connections",
    )
    parser.add_argument(
        "--cert",
        help="Path to server certificate PEM file",
    )
    parser.add_argument(
        "--key",
        help="Path to server private key PEM file",
    )
    parser.add_argument(
        "--self-signed",
        action="store_true",
        help="Generate a self-signed certificate for testing",
    )
    parser.add_argument(
        "--keepalive-timeout",
        type=float,
        default=75.0,
        help="Keep-alive timeout for client connections in seconds",
    )
    parser.add_argument(
        "--upstream-timeout",
        type=float,
        default=60.0,
        help="Timeout for upstream requests in seconds",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )

    parsed = parser.parse_args(args)

    if not parsed.self_signed and (not parsed.cert or not parsed.key):
        parser.error("Either specify --self-signed or provide both --cert and --key.")

    return ProxyConfig(
        host=parsed.host,
        port=parsed.port,
        cert=parsed.cert,
        key=parsed.key,
        self_signed=parsed.self_signed,
        keepalive_timeout=parsed.keepalive_timeout,
        upstream_timeout=parsed.upstream_timeout,
        log_level=parsed.log_level,
    )

