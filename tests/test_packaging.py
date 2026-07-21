"""Tests for plugin packaging (scripts/package_plugin.py).

All tests write archives to temporary directories so no source files or
build artifacts are modified.  Temp-repo tests copy the script to a
temporary plugin tree and execute the copy so that ROOT resolves to the
temporary root.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
PACKAGE_SCRIPT = PLUGIN_DIR / "scripts" / "package_plugin.py"

_METADATA_NAME = "astrbot_plugin_napcat_watchdog"
_METADATA_VERSION = "v0.1.0"
_PROJECT_VERSION = "0.1.0"

_PEP440_DEV_RE = re.compile(r"^\d+\.\d+\.\d+\.dev\d{12}$")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_script(*args: str) -> subprocess.CompletedProcess:
    """Run the *real* package script (ROOT points to actual plugin dir)."""
    return subprocess.run(
        [sys.executable, str(PACKAGE_SCRIPT), *args],
        capture_output=True,
        text=True,
    )


def _copy_script_to(plugin_dir: Path) -> Path:
    """Copy package_plugin.py to *plugin_dir*/scripts/."""
    scripts_dir = plugin_dir / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    shutil.copy2(PACKAGE_SCRIPT, scripts_dir / "package_plugin.py")
    return scripts_dir / "package_plugin.py"


def _run_script_at(plugin_dir: Path, *args: str) -> subprocess.CompletedProcess:
    """Run the package script from inside *plugin_dir*.

    The script is first copied into ``plugin_dir/scripts/`` so that
    ``ROOT = Path(__file__).resolve().parents[1]`` resolves to
    *plugin_dir*.
    """
    _copy_script_to(plugin_dir)
    script = plugin_dir / "scripts" / "package_plugin.py"
    return subprocess.run(
        [sys.executable, str(script), *args],
        capture_output=True,
        text=True,
        cwd=str(plugin_dir),
    )


def _get_release_zip(tmp_path: Path) -> Path:
    """Build a release package and return the zip path."""
    result = _run_script("--output-dir", str(tmp_path))
    assert result.returncode == 0, (
        f"Script failed:\nstdout:{result.stdout}\nstderr:{result.stderr}"
    )
    zips = list(tmp_path.glob("*.zip"))
    assert len(zips) == 1
    return zips[0]


def _get_dev_zip(tmp_path: Path, *, flat: bool = False) -> Path:
    """Build a dev-version package and return the zip path."""
    args = ["--dev-version", "--output-dir", str(tmp_path)]
    if flat:
        args.append("--flat")
    result = _run_script(*args)
    assert result.returncode == 0, (
        f"Script failed:\nstdout:{result.stdout}\nstderr:{result.stderr}"
    )
    zips = list(tmp_path.glob("*.zip"))
    assert len(zips) == 1
    return zips[0]


# ---------------------------------------------------------------------------
# Version consistency (direct parsing + full pipeline)
# ---------------------------------------------------------------------------


def test_metadata_and_project_versions_consistent(tmp_path: Path) -> None:
    """Directly verify metadata.yaml (v-prefixed) and pyproject.toml agree.

    Also verifies that formal preflight + packaging succeed when versions
    are consistent.
    """
    import tomllib
    import yaml

    meta = yaml.safe_load((PLUGIN_DIR / "metadata.yaml").read_text(encoding="utf-8"))
    pyproject = tomllib.loads(
        (PLUGIN_DIR / "pyproject.toml").read_text(encoding="utf-8")
    )

    meta_ver: str = meta["version"]  # e.g. "v0.1.0"
    proj_ver: str = pyproject["project"]["version"]  # e.g. "0.1.0"

    assert meta_ver.startswith("v"), (
        f"metadata version should have v prefix: {meta_ver}"
    )
    assert meta_ver.removeprefix("v") == proj_ver, (
        f"metadata version {meta_ver!r} stripped "
        f"({meta_ver.removeprefix('v')!r}) "
        f"!= project version {proj_ver!r}"
    )

    # Formal preflight + packaging must also succeed
    result = _run_script("--output-dir", str(tmp_path))
    assert result.returncode == 0, (
        f"Preflight/packaging failed:\nstdout:{result.stdout}\nstderr:{result.stderr}"
    )


# ---------------------------------------------------------------------------
# Release package structure
# ---------------------------------------------------------------------------


class TestReleasePackage:
    """Tests for ``python scripts/package_plugin.py`` (no flags)."""

    def test_filename_and_top_dir(self, tmp_path: Path) -> None:
        zip_path = _get_release_zip(tmp_path)
        assert zip_path.name == f"{_METADATA_NAME}-{_METADATA_VERSION}.zip"

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        top_dirs = [n for n in names if n.endswith("/")]
        assert len(top_dirs) == 1, f"Expected 1 top-level dir, got {top_dirs}"
        assert top_dirs[0] == f"{_METADATA_NAME}/"

    def test_required_root_files(self, tmp_path: Path) -> None:
        zip_path = _get_release_zip(tmp_path)
        prefix = f"{_METADATA_NAME}/"

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        required = [
            "main.py",
            "metadata.yaml",
            "_conf_schema.json",
            "requirements.txt",
            "pyproject.toml",
            "README.md",
            "LICENSE",
        ]
        for req in required:
            assert f"{prefix}{req}" in names, f"Missing required file: {prefix}{req}"

    def test_core_and_docs_dirs(self, tmp_path: Path) -> None:
        zip_path = _get_release_zip(tmp_path)
        prefix = f"{_METADATA_NAME}/"

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        core_files = [n for n in names if n.startswith(f"{prefix}core/")]
        assert len(core_files) > 0, f"No core/ files in archive:\n{names}"
        assert f"{prefix}core/__init__.py" in names, "Missing core/__init__.py"

        doc_files = [n for n in names if n.startswith(f"{prefix}docs/")]
        assert len(doc_files) > 0, f"No docs/ files in archive:\n{names}"
        assert f"{prefix}docs/PRD.md" in names, "Missing docs/PRD.md"

    def test_no_forbidden_content(self, tmp_path: Path) -> None:
        zip_path = _get_release_zip(tmp_path)

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        forbidden_patterns = [
            ".git",
            ".github",
            ".vscode",
            "tests",
            "tmp",
            "dist",
            "__pycache__",
            ".pytest_cache",
            ".ruff_cache",
        ]
        for n in names:
            for pattern in forbidden_patterns:
                assert pattern not in n, f"Forbidden path in archive: {n}"

    def test_metadata_content(self, tmp_path: Path) -> None:
        """Verify metadata.yaml inside the release archive is unmodified."""
        import yaml

        zip_path = _get_release_zip(tmp_path)
        prefix = f"{_METADATA_NAME}/"

        with zipfile.ZipFile(zip_path) as zf:
            data = yaml.safe_load(zf.read(f"{prefix}metadata.yaml"))

        assert data["name"] == _METADATA_NAME
        assert data["version"] == _METADATA_VERSION

    def test_pyproject_content(self, tmp_path: Path) -> None:
        """Verify pyproject.toml inside the release archive is unmodified."""
        import tomllib

        zip_path = _get_release_zip(tmp_path)
        prefix = f"{_METADATA_NAME}/"

        with zipfile.ZipFile(zip_path) as zf:
            data = tomllib.loads(zf.read(f"{prefix}pyproject.toml").decode("utf-8"))

        assert data["project"]["name"] == "astrbot-plugin-napcat-watchdog"
        assert data["project"]["version"] == _PROJECT_VERSION


# ---------------------------------------------------------------------------
# Dev-version package
# ---------------------------------------------------------------------------


class TestDevVersionPackage:
    """Tests for ``--dev-version`` flag."""

    def test_does_not_modify_source_files(self, tmp_path: Path) -> None:
        """Source metadata.yaml and pyproject.toml must remain untouched."""
        orig_meta = (PLUGIN_DIR / "metadata.yaml").read_text(encoding="utf-8")
        orig_pyproject = (PLUGIN_DIR / "pyproject.toml").read_text(encoding="utf-8")

        _get_dev_zip(tmp_path)

        assert (PLUGIN_DIR / "metadata.yaml").read_text(encoding="utf-8") == orig_meta
        assert (PLUGIN_DIR / "pyproject.toml").read_text(
            encoding="utf-8"
        ) == orig_pyproject

    def test_archive_has_dev_version(self, tmp_path: Path) -> None:
        """Archive's metadata and pyproject should be patched with dev version."""
        import tomllib
        import yaml

        zip_path = _get_dev_zip(tmp_path)
        assert ".dev" in zip_path.stem, (
            f"Expected dev version in filename: {zip_path.name}"
        )

        prefix = f"{_METADATA_NAME}/"

        with zipfile.ZipFile(zip_path) as zf:
            meta = yaml.safe_load(zf.read(f"{prefix}metadata.yaml"))
            proj = tomllib.loads(zf.read(f"{prefix}pyproject.toml").decode("utf-8"))

        archive_meta_ver: str = meta["version"]
        archive_proj_ver: str = proj["project"]["version"]

        # Metadata should keep v prefix
        assert archive_meta_ver.startswith("v"), (
            f"Metadata version should have v prefix: {archive_meta_ver}"
        )
        meta_dev = archive_meta_ver.removeprefix("v")
        assert _PEP440_DEV_RE.match(meta_dev), (
            f"Metadata dev version not PEP 440: {meta_dev}"
        )
        assert meta_dev == archive_proj_ver, (
            f"Metadata dev version ({meta_dev}) != project version ({archive_proj_ver})"
        )

    def test_pyproject_pep440_valid(self, tmp_path: Path) -> None:
        """pyproject.toml version in the archive must be valid PEP 440."""
        import tomllib

        zip_path = _get_dev_zip(tmp_path)
        prefix = f"{_METADATA_NAME}/"

        with zipfile.ZipFile(zip_path) as zf:
            proj = tomllib.loads(zf.read(f"{prefix}pyproject.toml").decode("utf-8"))

        proj_ver: str = proj["project"]["version"]
        # PEP 440 dev release: X.Y.Z.devYYYYMMDDHHMM
        assert _PEP440_DEV_RE.match(proj_ver), (
            f"Not valid PEP 440 dev version: {proj_ver}"
        )

    def test_filename_uses_dev_version(self, tmp_path: Path) -> None:
        zip_path = _get_dev_zip(tmp_path)
        stem = zip_path.stem
        assert stem.startswith(_METADATA_NAME)
        assert ".dev" in stem


# ---------------------------------------------------------------------------
# Flat mode
# ---------------------------------------------------------------------------


class TestFlatMode:
    """Tests for ``--flat`` flag."""

    def test_no_top_level_dir(self, tmp_path: Path) -> None:
        zip_path = _get_dev_zip(tmp_path, flat=True)

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        top_dirs = [n for n in names if n.endswith("/")]
        assert len(top_dirs) == 0, (
            f"Flat mode should have no top-level dirs: {top_dirs}"
        )

    def test_root_files_still_present(self, tmp_path: Path) -> None:
        zip_path = _get_dev_zip(tmp_path, flat=True)

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        for req in ("main.py", "metadata.yaml", "pyproject.toml", "README.md"):
            assert req in names, f"Missing {req} in flat archive"

    def test_core_files_flat(self, tmp_path: Path) -> None:
        zip_path = _get_dev_zip(tmp_path, flat=True)

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        core_py_files = [n for n in names if n.startswith("core/")]
        assert len(core_py_files) > 0, f"No core/ files in flat archive:\n{names}"


# ---------------------------------------------------------------------------
# Error cases – must fail closed
# ---------------------------------------------------------------------------


class TestErrorCases:
    """Tests that the packaging script fails closed on invalid input.

    Each test copies ``package_plugin.py`` into a temporary plugin tree so
    that ``ROOT`` resolves to that temporary tree.
    """

    def test_missing_required_files(self, tmp_path: Path) -> None:
        """Run from a temp dir missing pyproject.toml.

        The script should fail with ``FileNotFoundError`` caught by the
        ``__main__`` handler (printed as ``ERROR:`` on stderr).
        """
        temp_plugin = tmp_path / "partial_plugin"
        temp_plugin.mkdir()
        (temp_plugin / "main.py").write_text("")
        (temp_plugin / "metadata.yaml").write_text(
            "name: test_plugin\nversion: v1.0.0\n"
        )

        result = _run_script_at(temp_plugin, "--output-dir", str(tmp_path))
        assert result.returncode != 0
        assert "ERROR:" in result.stderr, (
            f"Expected 'ERROR:' prefix in stderr, got:\n{result.stderr}"
        )

    def test_version_mismatch_fails(self, tmp_path: Path) -> None:
        """If metadata and pyproject versions differ, release build fails."""
        temp_plugin = tmp_path / "mismatch_plugin"
        temp_plugin.mkdir()

        # Copy real plugin structure
        shutil.copytree(PLUGIN_DIR / "core", temp_plugin / "core")
        shutil.copytree(PLUGIN_DIR / "docs", temp_plugin / "docs")
        for f in (
            "main.py",
            "_conf_schema.json",
            "requirements.txt",
            "README.md",
            "LICENSE",
        ):
            shutil.copy2(PLUGIN_DIR / f, temp_plugin / f)

        # Standard metadata.yaml
        metadata_src = (PLUGIN_DIR / "metadata.yaml").read_text(encoding="utf-8")
        (temp_plugin / "metadata.yaml").write_text(metadata_src, encoding="utf-8")

        # Mismatched pyproject.toml version
        pyproject_src = (PLUGIN_DIR / "pyproject.toml").read_text(encoding="utf-8")
        pyproject_mismatch = pyproject_src.replace(
            'version = "0.1.0"', 'version = "0.2.0"'
        )
        (temp_plugin / "pyproject.toml").write_text(
            pyproject_mismatch, encoding="utf-8"
        )

        result = _run_script_at(temp_plugin, "--output-dir", str(tmp_path))
        assert result.returncode != 0
        # Error message should mention version mismatch
        assert "ERROR:" in result.stderr
        assert "version" in result.stderr.lower()


# ---------------------------------------------------------------------------
# CLI error handling in __main__
# ---------------------------------------------------------------------------


class TestCliErrorHandler:
    """Tests that the ``__main__`` block catches expected exceptions."""

    def test_missing_pyproject_file(self, tmp_path: Path) -> None:
        """FileNotFoundError is caught and printed as ``ERROR:``."""
        temp_plugin = tmp_path / "no_pyproject"
        temp_plugin.mkdir()
        (temp_plugin / "main.py").write_text("")
        (temp_plugin / "metadata.yaml").write_text(
            "name: p\ndisplay_name: P\ndesc: d\nversion: v0.1.0\n"
        )

        result = _run_script_at(temp_plugin, "--output-dir", str(tmp_path))
        assert result.returncode != 0
        assert "ERROR:" in result.stderr

    def test_invalid_metadata_not_mapping(self, tmp_path: Path) -> None:
        """ValueError for non-mapping metadata is caught."""
        temp_plugin = tmp_path / "bad_meta"
        temp_plugin.mkdir()
        (temp_plugin / "main.py").write_text("")
        (temp_plugin / "metadata.yaml").write_text("just a string\n")

        result = _run_script_at(temp_plugin, "--output-dir", str(tmp_path))
        assert result.returncode != 0
        assert "ERROR:" in result.stderr
