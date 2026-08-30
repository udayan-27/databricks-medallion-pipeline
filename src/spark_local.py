"""
Local SparkSession helpers.

Databricks: ``get_spark_session`` returns the active cluster session and never
applies these settings.

Local Windows: Hadoop 3.3.4 requires winutils.exe for ``setPermission`` and
``hadoop.dll`` NativeIO for ``listStatus`` during parquet commit. Those native
binaries are not part of this project. Instead, a small Java FileSystem
(``dec1.localfs.NoWinutilsRawLocalFileSystem``) is compiled with the same JDK
used for Spark and installed as ``fs.file.impl`` for locally created sessions.

Linux/macOS local Spark is left unchanged.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

_JAVA_SOURCE = (
    Path(__file__).resolve().parent / "local_runtime" / "NoWinutilsRawLocalFileSystem.java"
)
_FS_CLASS = "dec1.localfs.NoWinutilsRawLocalFileSystem"


def apply_local_spark_config(builder: Any) -> Any:
    """Attach local-only Hadoop FS config. No-op on non-Windows."""
    if os.name != "nt":
        return builder
    classpath = _compile_nowinutils_fs()
    return (
        builder.config("spark.driver.extraClassPath", classpath)
        .config("spark.executor.extraClassPath", classpath)
        .config("spark.hadoop.fs.file.impl", _FS_CLASS)
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    )


def _javac_path() -> Path:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / ("javac.exe" if os.name == "nt" else "javac")
        if candidate.is_file():
            return candidate
    found = shutil.which("javac")
    if found:
        return Path(found)
    raise RuntimeError(
        "javac not found. Local Windows Spark parquet writes compile a tiny "
        "Hadoop FileSystem that avoids winutils. Set JAVA_HOME to JDK 11 or 17."
    )


def _hadoop_client_api_jar() -> Path:
    import pyspark

    jars = Path(pyspark.__file__).resolve().parent / "jars"
    matches = sorted(jars.glob("hadoop-client-api-*.jar"))
    if not matches:
        raise RuntimeError(
            f"hadoop-client-api jar not found under {jars}. "
            "PySpark must be installed for local Spark validation."
        )
    return matches[-1]


def _compile_nowinutils_fs() -> str:
    if not _JAVA_SOURCE.is_file():
        raise RuntimeError(f"Missing local FileSystem source: {_JAVA_SOURCE}")
    out = Path(tempfile.gettempdir()) / "dec1_nowinutils_fs"
    out.mkdir(parents=True, exist_ok=True)
    class_file = out / "dec1" / "localfs" / "NoWinutilsRawLocalFileSystem.class"
    if class_file.is_file() and class_file.stat().st_mtime >= _JAVA_SOURCE.stat().st_mtime:
        return str(out)
    javac = _javac_path()
    jar = _hadoop_client_api_jar()
    cmd = [str(javac), "-classpath", str(jar), "-d", str(out), str(_JAVA_SOURCE)]
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(
            "Failed to compile local Windows Hadoop FileSystem "
            f"(exit {completed.returncode}): {detail}"
        )
    if not class_file.is_file():
        raise RuntimeError(f"javac succeeded but {class_file} is missing.")
    return str(out)
