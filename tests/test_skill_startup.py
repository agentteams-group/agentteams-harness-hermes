from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from hermes_worker.config import WorkerConfig
from hermes_worker.worker import Worker


def test_one_invalid_skill_does_not_prevent_healthy_skill_copy(tmp_path):
    worker = Worker(WorkerConfig("test-worker", "http://unused", "key", "secret", install_dir=tmp_path))
    worker.sync = SimpleNamespace(local_dir=worker.config.workspace_dir, list_skills=lambda: ["broken", "healthy", "missing"])
    for name, content in [("broken", "---\nname: [broken\n---\n"), ("healthy", "# Healthy\n")]:
        path = worker.sync.local_dir / "skills" / name / "SKILL.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    with patch("hermes_worker.worker.report_warnings") as report:
        worker._sync_skills()
    assert (worker._hermes_home / "skills/healthy/SKILL.md").read_text() == "# Healthy\n"
    assert not (worker._hermes_home / "skills/broken").exists()
    assert len(report.call_args.args[0]) == 2
    for name in ["broken", "missing"]:
        path = worker.sync.local_dir / "skills" / name / "SKILL.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# Repaired\n")
    with patch("hermes_worker.worker.report_warnings") as report:
        worker._sync_skills()
    assert report.call_args.args[0] == []
    assert (worker._hermes_home / "skills/broken/SKILL.md").is_file()


def test_skill_listing_outage_does_not_escape_initialization(tmp_path):
    worker = Worker(WorkerConfig("test-worker", "http://unused", "key", "secret", install_dir=tmp_path))
    def unavailable():
        raise OSError("unavailable")
    worker.sync = SimpleNamespace(list_skills=unavailable)
    with patch("hermes_worker.worker.report_warnings") as report:
        worker._sync_skills()
    assert "Skill sync failed" in report.call_args.args[0][0]
