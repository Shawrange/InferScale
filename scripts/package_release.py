"""Allowlisted source-only archive, with per-file hashes. No secrets/caches/models."""

import hashlib
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def package(root: Path) -> Path:
    paths = [
        root / name
        for name in [
            "README.md",
            "pyproject.toml",
            "uv.lock",
            ".dockerignore",
            ".gitignore",
            "InferScale_v1.0_技术设计文档.md",
        ]
    ]
    for folder, extensions in {
        "src": {".py"},
        "tests": {".py"},
        "scripts": {".py"},
        "configs": {".yaml", ".json"},
        "docs": {".md", ".json"},
        "deploy": {".yaml", ".md", ".example"},
    }.items():
        paths.extend(
            p
            for p in (root / folder).rglob("*")
            if p.is_file()
            and p.suffix in extensions
            and p.name != "server.yaml"
            and "__pycache__" not in p.parts
        )
    paths.append(root / "deploy/Dockerfile")
    destination = root / "dist/inferscale-p4-experiments.zip"
    destination.parent.mkdir(exist_ok=True)
    manifest = {}
    with ZipFile(destination, "w", ZIP_DEFLATED) as archive:
        for path in sorted(set(paths)):
            name = path.relative_to(root).as_posix()
            data = path.read_bytes()
            manifest[name] = hashlib.sha256(data).hexdigest()
            archive.writestr("inferscale/" + name, data)
        archive.writestr("inferscale/PACKAGE-MANIFEST.json", json.dumps(manifest, indent=2))
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    destination.with_suffix(".zip.sha256").write_text(
        f"{digest}  {destination.name}\n", encoding="ascii"
    )
    print(
        json.dumps(
            {
                "archive": str(destination),
                "files": len(manifest),
                "bytes": destination.stat().st_size,
                "sha256": digest,
            }
        )
    )
    return destination


if __name__ == "__main__":
    package(Path(__file__).resolve().parents[1])
