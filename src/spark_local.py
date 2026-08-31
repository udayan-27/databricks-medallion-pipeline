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
import time
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


_GATEWAY_PERMISSION_ATTEMPTS = 3
_TEMP_ENV_KEYS = ("TMP", "TEMP", "TMPDIR")


def start_local_test_spark(app_name: str, warehouse_dir: Path) -> Any:
    """
    Start a local SparkSession for unittest with isolated scratch directories.

    Warehouse and ``spark.local.dir`` are unique per test class. During JVM
    gateway launch, Python ``TEMP``/``TMP``/``TMPDIR`` are redirected into that
    scratch dir so Py4J's connection-info file is not created in the shared
    Windows temp folder.

    A PermissionError while opening that connection-info file is a known
    PySpark-on-Windows race (``java_gateway.launch_gateway``). This helper
    retries gateway launch only for that error. It does not retry assertion
    failures and does not change Gold/Silver/Bronze business logic.

    Local Spark tests in this repo are still sequential: do not run two full
    suites at once. Isolated dirs reduce TEMP collisions; they do not make
    concurrent suites a supported operating mode (shared CWD Derby metastore,
    CPU, and Windows file locks remain).
    """
    from pyspark.sql import SparkSession

    warehouse_dir.mkdir(parents=True, exist_ok=True)
    local_dir = warehouse_dir / "spark-local"
    local_dir.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", warehouse_dir.as_posix())
        .config("spark.local.dir", local_dir.as_posix())
    )
    builder = apply_local_spark_config(builder)
    last_error: BaseException | None = None
    for attempt in range(1, _GATEWAY_PERMISSION_ATTEMPTS + 1):
        existing = SparkSession.getActiveSession()
        if existing is not None:
            existing.stop()
        try:
            spark = _get_or_create_with_isolated_temp(builder, local_dir)
            spark.sparkContext.setLogLevel("ERROR")
            return spark
        except PermissionError as exc:
            last_error = exc
            time.sleep(float(attempt))
    assert last_error is not None
    raise last_error


def stop_local_test_spark(spark: Any, warehouse_dir: Path | None) -> None:
    """Stop a local test session and best-effort delete its warehouse."""
    if spark is not None:
        try:
            spark.stop()
        except Exception:
            pass
    if warehouse_dir is not None:
        shutil.rmtree(warehouse_dir, ignore_errors=True)


def _get_or_create_with_isolated_temp(builder: Any, local_dir: Path) -> Any:
    """Launch Spark with Python temp dirs pointed at ``local_dir``, then restore."""
    scratch = str(local_dir)
    previous = {key: os.environ.get(key) for key in _TEMP_ENV_KEYS}
    previous_spark_local = os.environ.get("SPARK_LOCAL_DIRS")
    try:
        for key in _TEMP_ENV_KEYS:
            os.environ[key] = scratch
        os.environ["SPARK_LOCAL_DIRS"] = scratch
        return builder.getOrCreate()
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        if previous_spark_local is None:
            os.environ.pop("SPARK_LOCAL_DIRS", None)
        else:
            os.environ["SPARK_LOCAL_DIRS"] = previous_spark_local


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
