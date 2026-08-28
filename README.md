# ext_proc_proxy - HTTPS/HTTP Proxy in Python

An asynchronous HTTPS-terminating proxy built with Python and [`aiohttp`](https://docs.aiohttp.org/).

## Features

- **HTTPS Termination**: Accepts incoming client connections over TLS/HTTPS on a configurable port.
- **Protocol Routing via `X-Forwarded-Proto`**:
  - `X-Forwarded-Proto: http` -> forwards outgoing request via **HTTP**.
  - `X-Forwarded-Proto: https` -> forwards outgoing request via **HTTPS**.
  - No `X-Forwarded-Proto` header -> defaults to **HTTPS**.
- **Client Keep-Alive**: Supports persistent HTTP/1.1 keep-alive connections on the client side.
- **Full Request/Response Streaming**: Asynchronously streams request payloads and response bodies without buffering entire payloads in memory.
- **Upstream TLS Verification**: Verifies target server TLS certificates when connecting over HTTPS.
- **Certificate Options**:
  - Load server certificate and private key from PEM files (`--cert` and `--key`).
  - Automatically generate a self-signed certificate on the fly for testing (`--self-signed`).
- **Clean Header Management**: Strips hop-by-hop headers and prevents propagation of `X-Forwarded-For` or `X-Forwarded-Host`.

---

## Installation & Requirements

- Python 3.9+ (Python 3.14 compatible)
- OpenSSL (for self-signed certificate generation)
- `aiohttp`

Dependencies are installed in the local virtual environment `.venv`.

---

## Usage

### Run with Self-Signed Certificate (Testing)

```bash
.venv/bin/python -m ext_proc_proxy --self-signed --port 8443
```

### Run with Custom Certificate and Key

```bash
.venv/bin/python -m ext_proc_proxy --cert /path/to/cert.pem --key /path/to/key.pem --port 8443
```

### Command-Line Arguments

| Flag | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | Host/IP address to bind the proxy server to |
| `-p`, `--port` | `8443` | Port to listen for incoming HTTPS connections |
| `--cert` | `None` | Path to server certificate PEM file |
| `--key` | `None` | Path to server private key PEM file |
| `--self-signed` | `False` | Generate self-signed certificate for testing |
| `--keepalive-timeout` | `75.0` | Keep-alive timeout for client connections in seconds |
| `--upstream-timeout` | `60.0` | Timeout for upstream requests in seconds |
| `--log-level` | `INFO` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

---

## Testing

Run all unit and integration tests:

```bash
.venv/bin/python -m unittest discover -s tests
```

