# Root conftest.py — ensures pytest can collect tests from all service subdirectories
# without namespace conflicts from __init__.py files in test folders.
import sys
import os

# Add all service roots to sys.path so tests can import service modules directly
for service in ["forecast", "ledger", "orchestrator_agent", "pricing_ingestor"]:
    service_path = os.path.join(os.path.dirname(__file__), "services", service)
    if service_path not in sys.path:
        sys.path.insert(0, service_path)

# Add packages to path
packages_path = os.path.join(os.path.dirname(__file__), "packages")
if packages_path not in sys.path:
    sys.path.insert(0, packages_path)
