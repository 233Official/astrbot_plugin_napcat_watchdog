#!/usr/bin/env python3
"""Package this AstrBot plugin into a zip archive for AstrBot WebUI upload."""

from __future__ import annotations

import argparse
import hashlib
import sys
import zipfile
from datetime import datetime
from pathlib import Path

try:
    import yaml
except ImportError as exc:  # pragma: no cover - PyYAML exists in CI/packaging envs.
    raise SystemExit("PyYAML is required to read metadata.yaml") from exc


ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist"

# Files required at the plugin root (relative to ROOT).
PACKAGE_ROOT_FILES = [
    "main.py",
    "metadata.yaml",
    "_conf_schema.json",
    "requirements.txt",
    "pyproject.toml",
    "README.md",
]

OPTIONAL_ROOT_FILES = [
    "LICENSE",
]

PACKAGE_DIRS = [
    "core",
    "docs",
]

EXCLUDE_DIRS = {
    "__pycache__",
    ".git",
    ".github",
    ".vscode",
    "tests",
    "tmp",
    "dist",
    "scripts",
    ".pytest_cache",
    ".ruff_cache",
}


def iter_package_files() -> list[str]:
    """Return relative paths of all files that should be included in the plugin archive
    according to the white-list rules."""
    files: list[str] = list(PACKAGE_ROOT_FILES)
    files.extend(path for path in OPTIONAL_ROOT_FILES if (ROOT / path).is_file())
    for dir_name in PACKAGE_DIRS:
        dir_path = ROOT / dir_name
        if dir_path.is_dir():
            for path in sorted(dir_path.rglob("*")):
                if not path.is_file():
                    continue
                rel = str(path.relative_to(ROOT))
                parts = set(path.relative_to(ROOT).parts)
                # Exclude known non-runtime directories and __pycache__
                if EXCLUDE_DIRS & parts:
                    continue
                if "__pycache__" in path.parts:
                    continue
                files.append(rel)
    return files


def read_metadata() -> dict:
    """Read plugin metadata from metadata.yaml.

    Returns:
        Parsed YAML metadata dict.

    Raises:
        ValueError: If metadata.yaml does not contain a YAML mapping.
    """
    metadata_path = ROOT / "metadata.yaml"
    with metadata_path.open("r", encoding="utf-8") as fh:
        metadata = yaml.safe_load(fh)
    if not isinstance(metadata, dict):
        raise ValueError("metadata.yaml must contain a YAML mapping")
    return metadata


def read_plugin_name() -> str:
    """Read plugin name from metadata.yaml.

    Returns:
        Plugin metadata name.

    Raises:
        ValueError: If the name is missing or empty.
    """
    name = read_metadata().get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("metadata.yaml must define a non-empty 'name'")
    return name.strip()


def read_plugin_version() -> str:
    """Read plugin version from metadata.yaml.

    Returns:
        Plugin metadata version (e.g. ``v0.1.0``).

    Raises:
        ValueError: If the version is missing or empty.
    """
    version = read_metadata().get("version")
    if not isinstance(version, str) or not version.strip():
        raise ValueError("metadata.yaml must define a non-empty 'version'")
    return version.strip()


def read_pyproject_project_info() -> dict:
    """Read ``[project]`` section from pyproject.toml.

    Returns:
        Dict with ``name`` and ``version`` keys.

    Raises:
        ValueError: If the section or required keys are missing.
    """
    try:
        import tomllib  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            raise SystemExit(
                "tomllib (Python 3.11+) or tomli required to parse pyproject.toml"
            )

    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    data = tomllib.loads(text)
    project = data.get("project")
    if not isinstance(project, dict):
        raise ValueError("pyproject.toml must contain a [project] section")
    errors: list[str] = []
    name = project.get("name")
    if not isinstance(name, str) or not name.strip():
        errors.append("pyproject.toml [project].name is missing or empty")
    version = project.get("version")
    if not isinstance(version, str) or not version.strip():
        errors.append("pyproject.toml [project].version is missing or empty")
    if errors:
        raise ValueError("; ".join(errors))
    return {"name": name, "version": version}


def validate_preflight(*, require_version_match: bool = True) -> None:
    """Validate metadata.yaml and pyproject.toml before building.

    Args:
        require_version_match: When True (release mode), require metadata
            version (with ``v`` prefix) to match the project version after
            stripping the prefix.

    Raises:
        ValueError: On any validation failure.
    """
    meta_name = read_plugin_name()
    meta_ver = read_plugin_version()
    proj = read_pyproject_project_info()

    errors: list[str] = []

    # metadata uses underscores (e.g. astrbot_plugin_napcat_watchdog),
    # pyproject uses hyphens (e.g. astrbot-plugin-napcat-watchdog).
    expected_proj_name = (
        f"astrbot-plugin-{meta_name.removeprefix('astrbot_plugin_').replace('_', '-')}"
    )
    if proj["name"] != expected_proj_name:
        errors.append(
            f"pyproject name {proj['name']!r} does not follow convention "
            f"{expected_proj_name}"
        )

    if require_version_match:
        expected_proj_ver = meta_ver.removeprefix("v")
        if proj["version"] != expected_proj_ver:
            errors.append(
                f"metadata version {meta_ver!r} stripped ({expected_proj_ver!r}) "
                f"!= pyproject version {proj['version']!r}"
            )

    if errors:
        raise ValueError("\n".join(errors))


def build_dev_version(base_version: str) -> str:
    """Return a PEP 440-compliant dev version based on local time.

    Result format: ``{base_without_v}.dev{YYYYMMDDHHMM}``.
    Example: ``0.1.0.dev202607221430``
    """
    stamp = datetime.now().strftime("%Y%m%d%H%M")
    base = base_version.removeprefix("v")
    return f"{base}.dev{stamp}"


def build_archive(
    output: Path,
    *,
    flat: bool,
    package_version: str | None = None,
) -> Path:
    """Build plugin zip archive.

    Args:
        output: Output zip path.
        flat: Whether to omit the top-level plugin directory inside the zip.
        package_version: Optional version to patch inside the archive only.
            Source files on disk are never modified.

    Returns:
        Output zip path.

    Raises:
        FileNotFoundError: If required source files are missing.
        ValueError: If version patching fails.
    """
    meta_version = read_plugin_version()
    plugin_name = read_plugin_name()
    pkg_ver = package_version or meta_version

    output.parent.mkdir(parents=True, exist_ok=True)

    files = iter_package_files()
    missing = [p for p in files if not (ROOT / p).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing package file(s): {', '.join(missing)}")

    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        if not flat:
            archive.writestr(f"{plugin_name}/", "")

        for relative in files:
            source = ROOT / relative
            archive_name = relative if flat else f"{plugin_name}/{relative}"

            if relative == "metadata.yaml" and pkg_ver != meta_version:
                metadata = read_metadata()
                # Keep the 'v' prefix convention for metadata
                metadata["version"] = f"v{pkg_ver.removeprefix('v')}"
                archive.writestr(
                    archive_name,
                    yaml.safe_dump(metadata, allow_unicode=True, sort_keys=False),
                )
            elif relative == "pyproject.toml" and pkg_ver != meta_version:
                text = source.read_text(encoding="utf-8")
                old = f'version = "{meta_version.removeprefix("v")}"'
                new = f'version = "{pkg_ver.removeprefix("v")}"'
                if old not in text:
                    raise ValueError(
                        f"Cannot patch project version in pyproject.toml: "
                        f"pattern {old!r} not found"
                    )
                archive.writestr(archive_name, text.replace(old, new, 1))
            else:
                archive.write(source, archive_name)

    return output


def sha256sum(path: Path) -> str:
    """Compute SHA-256 digest of a file."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def print_manifest(output: Path) -> None:
    """Print a short manifest of the built archive."""
    zipsize = output.stat().st_size
    sha = sha256sum(output)
    with zipfile.ZipFile(output) as zf:
        names = zf.namelist()
        prefix = next((n for n in names if n.endswith("/")), "")
        top_count = sum(1 for n in names if n == prefix or "/" not in n.rstrip("/"))
        print(f"Archive: {output.name}")
        print(f"  Size: {zipsize} bytes")
        print(f"  Entries: {len(names)}")
        if prefix:
            print(f"  Top-level dir: {prefix}")
        print(f"  SHA-256: {sha}")
        print(f"  Top-level files: {top_count}")
        print("  Contents:")
        for n in names:
            print(f"    {n}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Package the AstrBot NapCat Watchdog plugin into a zip archive.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Output directory. The filename is auto-derived as "
            "<metadata-name>-<metadata-version>.zip. "
            f"Defaults to {DIST_DIR}."
        ),
    )
    parser.add_argument(
        "--flat",
        action="store_true",
        help="Build a legacy flat archive without the top-level plugin directory.",
    )
    parser.add_argument(
        "--dev-version",
        action="store_true",
        help="Build a temporary test package with a PEP 440 dev prerelease version.",
    )
    parser.add_argument(
        "--package-version",
        type=str,
        default=None,
        help="Override the version written into the zip package.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run package command."""
    args = parse_args(sys.argv[1:] if argv is None else argv)
    package_version: str | None = args.package_version

    validate_preflight(require_version_match=not args.dev_version)

    if args.dev_version:
        if package_version:
            print("ERROR: --dev-version and --package-version cannot be used together")
            return 1
        package_version = build_dev_version(read_plugin_version())

    output_dir = args.output_dir or DIST_DIR
    meta_name = read_plugin_name()
    meta_version = read_plugin_version()
    pkg_ver = package_version or meta_version
    output = output_dir / f"{meta_name}-{pkg_ver}.zip"

    build_archive(output, flat=args.flat, package_version=package_version)
    print_manifest(output)
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
    except (ValueError, FileNotFoundError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        exit_code = 1
    raise SystemExit(exit_code)
