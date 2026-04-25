from __future__ import annotations

import importlib.util
import pathlib
import sys
from types import ModuleType


def _load_native() -> ModuleType:
    package_dir = pathlib.Path(__file__).resolve().parent
    project_root = package_dir.parent

    # Common library locations for local cargo builds and wheel installs.
    candidates = [
        project_root / "target" / "debug" / "libslingshot_microservice.so",
        project_root / "target" / "release" / "libslingshot_microservice.so",
    ]
    candidates.extend(package_dir.glob("_native*.so"))
    candidates.extend(package_dir.glob("libslingshot_microservice*.so"))

    module_name = "slingshot_microservice._native"
    for candidate in candidates:
        if not candidate.exists():
            continue

        spec = importlib.util.spec_from_file_location(module_name, candidate)
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    raise ModuleNotFoundError(
        "Native extension is not built. Build it with: cargo build --features python"
    )


_native = _load_native()
Microservice = _native.Microservice

__all__ = ["Microservice"]
