"""The production launcher must activate the packaged native Task adapter."""
import os
from pathlib import Path
import subprocess
import unittest


class LauncherProtocolTest(unittest.TestCase):
    def test_launcher_exports_plugin_directory_before_starting_runtime(self):
        launcher = Path(__file__).resolve().parents[1] / "scripts/hermes-worker-entrypoint.sh"
        bootstrap = "export AGENTTEAMS_OTEL_RUNTIME" + launcher.read_text().split("export AGENTTEAMS_OTEL_RUNTIME", 1)[1].split("WORKER_NAME=", 1)[0]
        for configured in [None, "/custom/teamharness"]:
            with self.subTest(configured=configured):
                env = dict(os.environ, AGENTTEAMS_CONTROLLER_URL="http://controller.test")
                env.pop("AGENTTEAMS_PLUGIN_DIR", None)
                if configured is not None:
                    env["AGENTTEAMS_PLUGIN_DIR"] = configured
                result = subprocess.run(["bash", "-c", "set -e\n" + bootstrap +
                    '\npython3 -c \'import os; assert os.getenv("AGENTTEAMS_CONTROLLER_URL") and os.getenv("AGENTTEAMS_PLUGIN_DIR"); print(os.environ["AGENTTEAMS_PLUGIN_DIR"])\''],
                    env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), configured or "/opt/agentteams/plugins/teamharness")
