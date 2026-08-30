"""ext_proc_proxy - HTTPS/HTTP reverse and forward proxy with Envoy ext_proc support."""

import os
import sys

# Ensure generated protobuf stubs are importable
GEN_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.gen"))
if os.path.exists(GEN_DIR) and GEN_DIR not in sys.path:
    sys.path.insert(0, GEN_DIR)
