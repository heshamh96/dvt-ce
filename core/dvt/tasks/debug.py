"""
DVT Debug Task — check connections to all targets or a specific target.

dvt debug              → tests ALL connections in profiles.yml
dvt debug --target X   → tests only connection X
"""

import time
from typing import Any, Dict, List, Optional, Tuple

from dvt.extraction.connection_mapper import map_to_sling_url, supported_types
from dvt.sync.profiles_reader import (
    default_profiles_dir,
    extract_outputs,
    read_profiles_yml,
)
from dvt.sync.sling_checker import check_sling


class DvtDebugTask:
    """Check connections to all targets in profiles.yml."""

    def __init__(self, flags: Any) -> None:
        self.flags = flags
        self.profiles_dir = (
            getattr(flags, "PROFILES_DIR", None) or default_profiles_dir()
        )
        self.target_filter = getattr(flags, "TARGET", None)

    def run(self) -> Any:
        print(f"\ndvt debug — checking connections from: {self.profiles_dir}\n")

        # Read profiles.yml
        try:
            raw_profiles = read_profiles_yml(self.profiles_dir)
        except FileNotFoundError as e:
            print(f"  ERROR: {e}")
            return {"success": False}
        except Exception as e:
            print(f"  ERROR reading profiles.yml: {e}")
            return {"success": False}

        outputs = extract_outputs(raw_profiles)
        if not outputs:
            print("  No outputs found in profiles.yml.")
            return {"success": True}

        # Filter to specific target if --target is set
        if self.target_filter:
            outputs = [o for o in outputs if o.get("_name") == self.target_filter]
            if not outputs:
                print(f"  Target '{self.target_filter}' not found in profiles.yml.")
                return {"success": False}

        # Check Sling availability
        sling_ok, sling_version = check_sling()
        print(f"  Sling: {'sling ' + sling_version if sling_ok else 'NOT FOUND'}")

        # Check DuckDB
        try:
            import duckdb

            print(f"  DuckDB: {duckdb.__version__}")
        except ImportError:
            print("  DuckDB: NOT FOUND")
        print()

        # Test each connection (both adapter and Sling)
        all_ok = True
        results: List[Tuple[str, str, str, str]] = []  # (name, type, status, detail)

        print("  Connections:")
        for output in outputs:
            name = output.get("_name", "?")
            adapter_type = output.get("type", "?")

            # Test adapter (native driver)
            adapter_status, adapter_detail, adapter_elapsed = (
                self._test_adapter(output)
            )

            # Test Sling
            sling_status, sling_detail, sling_elapsed = (
                self._test_sling(output)
            )

            # Overall status: OK if adapter passes
            elapsed = max(adapter_elapsed, sling_elapsed)
            if adapter_status == "ok":
                status = "ok"
            elif adapter_status == "skip":
                # Adapter not available — rely on Sling result
                status = sling_status
            else:
                status = "failed"
            all_ok = all_ok and status == "ok"
            results.append((name, adapter_type, status, ""))

            pad = "." * max(1, 30 - len(name))
            elapsed_str = f" ({elapsed:.1f}s)" if elapsed else ""

            # Format adapter status
            a_icon = "OK" if adapter_status == "ok" else (
                "SKIP" if adapter_status == "skip" else "FAIL"
            )
            s_icon = "OK" if sling_status == "ok" else (
                "SKIP" if sling_status == "skip" else "FAIL"
            )

            if status == "ok":
                print(f"    🟩 {name} {pad} {adapter_type}{elapsed_str}")
                print(f"       Adapter: {a_icon} | Sling: {s_icon}")
            elif status == "skip":
                print(f"    ⬜ {name} {pad} {adapter_type}")
                print(f"       Adapter: {a_icon} | Sling: {s_icon}")
            else:
                print(f"    🟥 {name} {pad} {adapter_type}")
                detail = adapter_detail or sling_detail
                print(f"       Adapter: {a_icon} | Sling: {s_icon}")
                if detail:
                    print(f"       {detail}")

        print()
        ok_count = sum(1 for _, _, s, _ in results if s == "ok")
        fail_count = sum(1 for _, _, s, _ in results if s == "failed")
        skip_count = sum(1 for _, _, s, _ in results if s == "skip")
        total = len(results)

        if all_ok:
            print(f"  All {ok_count} connection(s) OK.")
        else:
            print(
                f"  {ok_count} OK, {fail_count} FAILED, {skip_count} SKIPPED out of {total}."
            )

        return {"success": all_ok, "results": results}

    def _test_adapter(
        self, output: Dict[str, Any]
    ) -> Tuple[str, str, float]:
        """Test connection using the native database driver."""
        from dvt.tasks.docs import _get_connection

        adapter_type = output.get("type", "")
        if adapter_type in ("s3", "gcs", "azure"):
            return self._test_bucket(output)

        try:
            start = time.time()
            conn = _get_connection(adapter_type, output)
            if not conn:
                return "skip", f"no driver for {adapter_type}", 0.0
            cursor = conn.cursor()
            if "oracle" in adapter_type:
                cursor.execute("SELECT 1 FROM DUAL")
            else:
                cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            elapsed = time.time() - start
            return "ok", "", elapsed
        except Exception as e:
            elapsed = time.time() - start if "start" in dir() else 0.0
            msg = str(e)
            if "could not connect" in msg.lower() or "connection refused" in msg.lower():
                return "failed", "could not connect to database", elapsed
            return "failed", msg[:200], elapsed

    def _test_sling(
        self, output: Dict[str, Any]
    ) -> Tuple[str, str, float]:
        """Test connection using Sling."""
        adapter_type = output.get("type", "")
        if adapter_type in ("s3", "gcs", "azure"):
            return "skip", "bucket", 0.0

        if adapter_type not in supported_types():
            return "skip", f"unsupported type", 0.0

        sling_ok, _ = check_sling()
        if not sling_ok:
            return "skip", "Sling not available", 0.0

        try:
            url = map_to_sling_url(output)
        except Exception as e:
            return "failed", f"bad URL: {e}", 0.0

        name = output.get("_name", "?")
        status, detail, elapsed = self._test_db_via_sling_replication(
            name, url, adapter_type
        )

        # If Sling failed and URL has sslmode=prefer, retry with sslmode=disable
        # Some platforms (WSL2, Docker) don't support SSL negotiation
        if status == "failed" and "sslmode=prefer" in url:
            url_fallback = url.replace("sslmode=prefer", "sslmode=disable")
            status, detail, elapsed = self._test_db_via_sling_replication(
                name, url_fallback, adapter_type
            )

        return status, detail, elapsed

    def _test_db_via_sling_replication(
        self, name: str, url: str, adapter_type: str
    ) -> Tuple[str, str, float]:
        """Test a database connection via Sling Replication with a SELECT 1 query."""
        try:
            from sling import Replication, ReplicationStream
            import tempfile
            import os

            start = time.time()
            # Create a minimal replication that just reads SELECT 1 from the source
            # and writes to a temp file
            tmp = tempfile.mktemp(suffix=".csv")
            try:
                # Use dialect-appropriate ping query
                if "oracle" in adapter_type:
                    ping_sql = "SELECT 1 FROM DUAL"
                else:
                    ping_sql = "SELECT 1"

                replication = Replication(
                    source=url,
                    target=f"file://.",
                    streams={
                        "custom_sql": ReplicationStream(
                            sql=ping_sql,
                            object=tmp,
                            mode="full-refresh",
                        ),
                    },
                )
                import io
                import sys

                # Suppress Sling stdout/stderr (config file path, etc.)
                old_stdout, old_stderr = sys.stdout, sys.stderr
                sys.stdout = io.StringIO()
                sys.stderr = io.StringIO()
                try:
                    replication.run(return_output=True)
                finally:
                    sys.stdout, sys.stderr = old_stdout, old_stderr
                elapsed = time.time() - start
                return "ok", "", elapsed
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

        except Exception as e:
            elapsed = time.time() - start if "start" in dir() else 0.0
            error_msg = str(e)
            # Extract just the useful part of the error
            if "could not connect" in error_msg.lower():
                return "failed", "could not connect to database", elapsed
            return "failed", error_msg[:200], elapsed

    def _test_bucket(self, output: Dict[str, Any]) -> Tuple[str, str, float]:
        """Test a bucket connection (S3, GCS, Azure)."""
        adapter_type = output.get("type", "")

        if adapter_type == "s3":
            try:
                import boto3

                start = time.time()
                bucket = output.get("bucket", "")
                region = output.get("region", "us-east-1")
                s3 = boto3.client(
                    "s3",
                    region_name=region,
                    aws_access_key_id=output.get("access_key_id"),
                    aws_secret_access_key=output.get("secret_access_key"),
                )
                s3.head_bucket(Bucket=bucket)
                elapsed = time.time() - start
                return "ok", "", elapsed
            except ImportError:
                return "skip", "boto3 not installed", 0.0
            except Exception as e:
                return "failed", str(e)[:200], 0.0

        elif adapter_type == "gcs":
            try:
                from google.cloud import storage

                start = time.time()
                client = storage.Client(project=output.get("project"))
                bucket = client.get_bucket(output.get("bucket", ""))
                elapsed = time.time() - start
                return "ok", "", elapsed
            except ImportError:
                return "skip", "google-cloud-storage not installed", 0.0
            except Exception as e:
                return "failed", str(e)[:200], 0.0

        return "skip", f"bucket type '{adapter_type}' not testable", 0.0

    @staticmethod
    def interpret_results(results: Any) -> bool:
        if results is None:
            return False
        if isinstance(results, dict):
            return results.get("success", False)
        return True
