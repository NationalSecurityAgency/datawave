#!/usr/bin/env python3
"""Summarize JUnit XML results, highlighting flaky tests (passed only after a rerun).

Surefire/Failsafe record a flake as a <testcase> that ultimately passed but
contains <flakyFailure>/<flakyError> child elements (produced when
-Dsurefire.rerunFailingTestsCount / -Dfailsafe.rerunFailingTestsCount is set).
Those flakes are otherwise hidden from the build's pass/fail status, so this
script surfaces them in the GitHub Actions job summary and as log annotations.

Uses only the Python standard library so no extra setup is needed on the runner.
Non-gating by default: exits 0 unless --fail-on-failure or --fail-on-flaky is given.

Usage:
    python3 junit_flaky_report.py --title "Main Repo Test Report" \
        '**/target/*-reports/TEST-*.xml'
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Optional

# GitHub renders at most ~10 annotations of each type per step in the PR UI;
# cap emission so the log isn't flooded while the summary still lists everything.
MAX_ANNOTATIONS = 50


@dataclass
class TestCase:
    name: str
    classname: str
    time: float = 0.0
    status: str = "passed"  # passed | failed | error | skipped
    flaky_count: int = 0

    @property
    def display(self) -> str:
        if self.classname and not self.name.startswith(self.classname):
            return f"{self.classname}.{self.name}"
        return self.name or self.classname or "(unknown)"


@dataclass
class Totals:
    tests: int = 0
    failures: int = 0
    errors: int = 0
    skipped: int = 0
    flaky: int = 0
    time: float = 0.0
    flaky_cases: List[TestCase] = field(default_factory=list)
    failed_cases: List[TestCase] = field(default_factory=list)


def _to_float(value: Optional[str]) -> float:
    try:
        return float(value) if value else 0.0
    except ValueError:
        return 0.0


def parse_file(path: str, totals: Totals) -> None:
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError) as exc:
        print(f"::warning::Could not parse {path}: {exc}")
        return
    suites = [root] if root.tag == "testsuite" else root.iter("testsuite")
    for suite in suites:
        for tc in suite.findall("testcase"):
            case = TestCase(
                name=tc.get("name", ""),
                classname=tc.get("classname", ""),
                time=_to_float(tc.get("time")),
            )
            has_failure = tc.find("failure") is not None
            has_error = tc.find("error") is not None
            has_skipped = tc.find("skipped") is not None
            flaky = len(tc.findall("flakyFailure")) + len(tc.findall("flakyError"))

            totals.tests += 1
            totals.time += case.time
            if has_failure:
                case.status = "failed"
                totals.failures += 1
                totals.failed_cases.append(case)
            elif has_error:
                case.status = "error"
                totals.errors += 1
                totals.failed_cases.append(case)
            elif has_skipped:
                case.status = "skipped"
                totals.skipped += 1
            # A flake is a testcase that ultimately passed but was retried; record
            # it independently of the pass/fail bucket above.
            if flaky:
                case.flaky_count = flaky
                totals.flaky += 1
                totals.flaky_cases.append(case)


def format_summary(title: str, totals: Totals) -> str:
    if totals.failures or totals.errors:
        status = "FAILED"
    elif totals.flaky:
        status = "FLAKY"
    else:
        status = "PASSED"

    lines = [
        f"## {title}",
        "",
        f"**{status}** - **{totals.tests}** tests, {totals.failures} failed, "
        f"{totals.errors} errored, {totals.skipped} skipped, "
        f"**{totals.flaky} flaky** in {totals.time:.1f}s",
    ]

    if totals.flaky_cases:
        lines += [
            "",
            "### Flaky tests (passed only after a rerun)",
            "",
            "| Test | Reruns |",
            "| --- | --- |",
        ]
        for c in sorted(totals.flaky_cases, key=lambda x: x.display):
            lines.append(f"| {c.display} | {c.flaky_count} |")

    if totals.failed_cases:
        lines += [
            "",
            "### Failed tests",
            "",
            "| Test | Type |",
            "| --- | --- |",
        ]
        for c in sorted(totals.failed_cases, key=lambda x: x.display):
            lines.append(f"| {c.display} | {c.status} |")

    if not totals.flaky_cases and not totals.failed_cases:
        lines += ["", "No failures or flakes detected."]

    lines.append("")
    return "\n".join(lines)


def write_summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(text)
        if not text.endswith("\n"):
            fh.write("\n")


def emit_annotations(totals: Totals) -> None:
    for c in totals.flaky_cases[:MAX_ANNOTATIONS]:
        print(
            f"::warning title=Flaky test::{c.display} passed only after "
            f"{c.flaky_count} rerun(s)"
        )
    for c in totals.failed_cases[:MAX_ANNOTATIONS]:
        print(f"::error title=Test {c.status}::{c.display}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Summarize JUnit XML, highlighting flaky tests."
    )
    parser.add_argument(
        "patterns", nargs="+", help="Glob(s) for JUnit XML files (matched recursively)."
    )
    parser.add_argument(
        "--title", default="Test Report", help="Heading used in the job summary."
    )
    parser.add_argument(
        "--fail-on-failure",
        action="store_true",
        help="Exit non-zero if any test failed or errored.",
    )
    parser.add_argument(
        "--fail-on-flaky",
        action="store_true",
        help="Exit non-zero if any flaky test was detected.",
    )
    args = parser.parse_args(argv)

    files = sorted({f for pat in args.patterns for f in glob.glob(pat, recursive=True)})

    totals = Totals()
    if not files:
        msg = f"No JUnit XML matched: {', '.join(args.patterns)}"
        print(f"::warning::{msg}")
        write_summary(f"## {args.title}\n\n{msg}\n")
        return 0

    for path in files:
        parse_file(path, totals)

    summary = format_summary(args.title, totals)
    write_summary(summary)
    print(summary)
    emit_annotations(totals)

    rc = 0
    if args.fail_on_failure and (totals.failures or totals.errors):
        rc = 1
    if args.fail_on_flaky and totals.flaky:
        rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
