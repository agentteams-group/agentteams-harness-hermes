"""Make ``hermes/src`` importable for the worker tests (bare pytest)."""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
source = Path(os.environ.get("AT_SOURCE", Path(__file__).resolve().parents[1] / ".deps/AgentTeams"))
sys.path.insert(0, str(source / "shared/lib"))
