# c:\Grow\tests\run_all_suites.py
"""
GROW TREASURY PLATFORM — MASTER END-TO-END VERIFICATION SUITE RUNNER
====================================================================
Executes the LG Custody, LG Issuance/Facilities/Maintenance/Reconciliation,
and Platform Modules end-to-end verification suites sequentially in isolated
Python subprocesses (preventing startup DDL lock contention), asserting exact
user-expected outputs ("does the job", not just HTTP 200).

Usage:
    c:\\Grow\\venv\\Scripts\\python.exe c:\\Grow\\tests\\run_all_suites.py              # Runs all new Custody + Issuance + Platform suites
    c:\\Grow\\venv\\Scripts\\python.exe c:\\Grow\\tests\\run_all_suites.py --custody    # LG Custody suites only
    c:\\Grow\\venv\\Scripts\\python.exe c:\\Grow\\tests\\run_all_suites.py --issuance   # LG Issuance, Facilities, Maintenance & Recon only
    c:\\Grow\\venv\\Scripts\\python.exe c:\\Grow\\tests\\run_all_suites.py --platform   # Auth v2, Governance, Inbox, Campaigns, FX & Recon v2
    c:\\Grow\\venv\\Scripts\\python.exe c:\\Grow\\tests\\run_all_suites.py --existing   # Pre-existing Quotations/Crypto suites
    c:\\Grow\\venv\\Scripts\\python.exe c:\\Grow\\tests\\run_all_suites.py --all        # Everything (New + Pre-existing)
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List


TESTS_DIR = os.path.abspath(os.path.dirname(__file__))
REPO_ROOT = os.path.abspath(os.path.join(TESTS_DIR, ".."))

SUITE_GROUPS = {
    "custody": [
        (
            "LG Custody Lifecycle, PDF, Rollback & QR Seal",
            os.path.join(TESTS_DIR, "test_lg_custody_omni_lifecycle_suite.py"),
        ),
        (
            "LG Custody Delivery AI, 5-Branch Reply, Migration, Reports & Crons",
            os.path.join(TESTS_DIR, "test_lg_custody_delivery_ai_migration_reports_suite.py"),
        ),
    ],
    "issuance": [
        (
            "LG Issuance, Credit Facilities, SLA EMA, Maintenance, Recon & Export",
            os.path.join(TESTS_DIR, "test_issuance_facilities_maintenance_recon_suite.py"),
        ),
    ],
    "platform": [
        (
            "Platform Modules (Auth v2, Spillover, WORM, Inbox, Cashback, FX, Recon v2)",
            os.path.join(TESTS_DIR, "test_platform_modules_e2e_suite.py"),
        ),
    ],
    "existing": [
        (
            "Production Readiness (Isolation, Crypto, Regression, Recovery)",
            os.path.join(TESTS_DIR, "test_production_readiness_suite.py"),
        ),
        (
            "User Expectations Journeys",
            os.path.join(TESTS_DIR, "test_user_expectations_journeys.py"),
        ),
        (
            "Omni Simulation & Adversarial Quotations Suite",
            os.path.join(TESTS_DIR, "test_omni_simulation_suite.py"),
        ),
    ],
}


def run_suite_subprocess(label: str, script_path: str, verbose: bool = False) -> Dict[str, Any]:
    print(f"\n{'─' * 96}")
    print(f"▶ RUNNING: {label}")
    print(f"  Script : {script_path}")
    print(f"{'─' * 96}", flush=True)

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = REPO_ROOT

    t0 = time.perf_counter()
    proc = subprocess.Popen(
        [sys.executable, script_path],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    pass_lines: List[str] = []
    audit_lines: List[str] = []
    all_lines: List[str] = []

    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.rstrip("\r\n")
        all_lines.append(line)
        if "[PASS]" in line:
            pass_lines.append(line.strip())
            print(f"  {line.strip()}", flush=True)
        elif "[AUDIT FINDING]" in line:
            audit_lines.append(line.strip())
            print(f"  {line.strip()}", flush=True)
        elif verbose:
            print(line, flush=True)

    exit_code = proc.wait()
    duration_s = round(time.perf_counter() - t0, 2)

    if exit_code != 0 and not verbose:
        print("\n  [FAILURE TAIL OUTPUT]:")
        for tail_line in all_lines[-40:]:
            print(f"    {tail_line}")

    status_str = "PASS" if exit_code == 0 else "FAIL"
    print(f"  ──► Result: {status_str} (exit={exit_code}, {len(pass_lines)} checkpoints, {duration_s:.2f}s)", flush=True)

    return {
        "label": label,
        "script": os.path.basename(script_path),
        "status": status_str,
        "exit_code": exit_code,
        "checkpoints_passed": len(pass_lines),
        "audit_findings": audit_lines,
        "duration_seconds": duration_s,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Grow Treasury Master E2E Suite Runner")
    parser.add_argument("--custody", action="store_true", help="Run LG Custody verification suites")
    parser.add_argument("--issuance", action="store_true", help="Run LG Issuance, Facilities, Maintenance & Recon suite")
    parser.add_argument("--platform", action="store_true", help="Run Auth v2, Governance, Inbox, Campaigns, FX & Recon v2 suite")
    parser.add_argument("--existing", action="store_true", help="Run pre-existing Quotations & Production Readiness suites")
    parser.add_argument("--all", action="store_true", help="Run ALL suites (new + pre-existing)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Stream full HTTP/SQL logs")
    parser.add_argument("--json-out", type=str, default=None, help="Optional path to save JSON summary report")
    args = parser.parse_args()

    selected_groups: List[str] = []
    if args.all:
        selected_groups = ["custody", "issuance", "platform", "existing"]
    else:
        if args.custody:
            selected_groups.append("custody")
        if args.issuance:
            selected_groups.append("issuance")
        if args.platform:
            selected_groups.append("platform")
        if args.existing:
            selected_groups.append("existing")
        if not selected_groups:
            # Default: run all 4 new comprehensive Custody, Issuance & Platform suites
            selected_groups = ["custody", "issuance", "platform"]

    suites_to_run = []
    for grp in selected_groups:
        for label, path in SUITE_GROUPS[grp]:
            if os.path.isfile(path):
                suites_to_run.append((label, path))

    print("=" * 96)
    print("GROW TREASURY PLATFORM — MASTER VERIFICATION MATRIX")
    print(f"Selected Groups : {', '.join(selected_groups)} ({len(suites_to_run)} suite files)")
    print("=" * 96, flush=True)

    overall_t0 = time.perf_counter()
    results: List[Dict[str, Any]] = []
    for label, path in suites_to_run:
        results.append(run_suite_subprocess(label, path, verbose=args.verbose))

    total_duration = round(time.perf_counter() - overall_t0, 2)
    total_passed = sum(1 for r in results if r["exit_code"] == 0)
    total_checkpoints = sum(r["checkpoints_passed"] for r in results)

    print("\n" + "=" * 96)
    print("EXECUTIVE VERIFICATION SUMMARY")
    print("=" * 96)
    print(f"{'Suite File':<54} | {'Status':<6} | {'Checks':>6} | {'Time (s)':>8}")
    print("-" * 96)
    for r in results:
        print(f"{r['script']:<54} | {r['status']:<6} | {r['checkpoints_passed']:>6} | {r['duration_seconds']:>8.2f}")
    print("-" * 96)
    print(
        f"TOTAL: {total_passed}/{len(results)} Suites Passed | "
        f"{total_checkpoints} Verified Output Checkpoints | {total_duration:.2f}s Elapsed"
    )
    print("=" * 96, flush=True)

    if args.json_out:
        os.makedirs(os.path.dirname(os.path.abspath(args.json_out)), exist_ok=True)
        with open(args.json_out, "w", encoding="utf-8") as jf:
            json.dump(
                {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "suites_passed": total_passed,
                    "suites_total": len(results),
                    "total_checkpoints": total_checkpoints,
                    "total_duration_seconds": total_duration,
                    "results": results,
                },
                jf,
                indent=2,
            )

    return 0 if total_passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
