"""Tests for configuration parsing."""

import unittest

from ext_proc_proxy.config import ProxyConfig, parse_args


class TestConfig(unittest.TestCase):
    """Test CLI argument parsing and config defaults."""

    def test_default_config(self):
        """Test default config with self_signed flag."""
        config = parse_args(["--self-signed"])
        self.assertEqual(config.host, "0.0.0.0")
        self.assertEqual(config.port, 8443)
        self.assertTrue(config.self_signed)
        self.assertEqual(config.keepalive_timeout, 75.0)
        self.assertEqual(config.upstream_timeout, 60.0)
        self.assertEqual(config.log_level, "INFO")

    def test_custom_config(self):
        """Test custom CLI arguments."""
        config = parse_args(
            [
                "--host",
                "127.0.0.1",
                "-p",
                "9443",
                "--cert",
                "/path/to/cert.pem",
                "--key",
                "/path/to/key.pem",
                "--keepalive-timeout",
                "30.0",
                "--upstream-timeout",
                "15.0",
                "--log-level",
                "DEBUG",
            ]
        )
        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 9443)
        self.assertEqual(config.cert, "/path/to/cert.pem")
        self.assertEqual(config.key, "/path/to/key.pem")
        self.assertFalse(config.self_signed)
        self.assertEqual(config.keepalive_timeout, 30.0)
        self.assertEqual(config.upstream_timeout, 15.0)
        self.assertEqual(config.log_level, "DEBUG")

    def test_missing_cert_and_self_signed(self):
        """Test error when neither cert/key nor --self-signed is given."""
        with self.assertRaises(SystemExit):
            parse_args([])


if __name__ == "__main__":
    unittest.main()

