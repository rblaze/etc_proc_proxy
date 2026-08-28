"""Tests for certificate utilities."""

import os
import ssl
import tempfile
import unittest

from ext_proc_proxy.cert_utils import (
    create_server_ssl_context,
    generate_self_signed_cert,
)


class TestCertUtils(unittest.TestCase):
    """Test certificate generation and SSL context creation."""

    def test_generate_self_signed_cert_temp(self):
        """Test generating self-signed certificate in temporary directory."""
        cert_path, key_path = generate_self_signed_cert()
        try:
            self.assertTrue(os.path.exists(cert_path))
            self.assertTrue(os.path.exists(key_path))
            self.assertGreater(os.path.getsize(cert_path), 0)
            self.assertGreater(os.path.getsize(key_path), 0)

            # Check that SSLContext can load the generated cert and key
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
        finally:
            if os.path.exists(cert_path):
                os.remove(cert_path)
            if os.path.exists(key_path):
                os.remove(key_path)

    def test_generate_self_signed_cert_custom_path(self):
        """Test generating self-signed certificate at specified paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            cert_path = os.path.join(temp_dir, "custom_cert.pem")
            key_path = os.path.join(temp_dir, "custom_key.pem")

            res_cert, res_key = generate_self_signed_cert(
                cert_path=cert_path,
                key_path=key_path,
                hostname="test.example.com",
            )
            self.assertEqual(res_cert, cert_path)
            self.assertEqual(res_key, key_path)
            self.assertTrue(os.path.exists(cert_path))
            self.assertTrue(os.path.exists(key_path))

            ctx = create_server_ssl_context(
                cert_file=cert_path, key_file=key_path
            )
            self.assertIsInstance(ctx, ssl.SSLContext)

    def test_create_server_ssl_context_with_self_signed_flag(self):
        """Test creating SSLContext directly with generate_self_signed=True."""
        ctx = create_server_ssl_context(generate_self_signed=True)
        self.assertIsInstance(ctx, ssl.SSLContext)
        self.assertEqual(ctx.minimum_version, ssl.TLSVersion.TLSv1_2)

    def test_create_server_ssl_context_missing_files(self):
        """Test error handling when cert or key files are missing."""
        with self.assertRaises(ValueError):
            create_server_ssl_context(cert_file=None, key_file=None)

        with self.assertRaises(FileNotFoundError):
            create_server_ssl_context(
                cert_file="/nonexistent/cert.pem", key_file="/nonexistent/key.pem"
            )


if __name__ == "__main__":
    unittest.main()

