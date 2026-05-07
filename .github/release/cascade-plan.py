#!/usr/bin/env python3
"""Compute the cascade-release plan for the DataWave microservices.

Maven's `-pl :artifact -amd` reactor selection cannot be used here because
consumer POMs pin released versions of their inter-microservice dependencies
(e.g. ``<version.datawave.audit-api>4.0.2</...>``) while the producer's POM
declares a different SNAPSHOT version. As a result the reactor does not link
consumer ↔ producer and `-amd` returns only the starting module.

This script walks every ``pom.xml`` under ``microservices/`` (skipping
``target``/``src``/``configcheck``), records each project's ``artifactId``,
its ``<parent>`` reference, and every ``<dependency>`` whose ``groupId`` is
``gov.nsa.datawave.microservice`` (regardless of version), builds the
consumer graph in memory, and prints the starting project plus its transitive
dependents in a deterministic topological order as a JSON array of
``{artifactId, path}`` records.

The output is consumed by ``.github/workflows/microservice-cascade-release.yml``
to drive the per-project release loop.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple
from xml.etree import ElementTree as ET

MS_GROUP_ID = "gov.nsa.datawave.microservice"
MAVEN_NS = "http://maven.apache.org/POM/4.0.0"
NS = {"m": MAVEN_NS}

# .github/release/cascade-plan.py -> repo root is two levels up.
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MICROSERVICES_ROOT = REPO_ROOT / "microservices"
SKIP_SEGMENTS = {"target", "src", "configcheck"}


def _text(elem) -> str:
    return (elem.text or "").strip() if elem is not None else ""


def _iter_poms() -> Iterable[Path]:
    for pom in MICROSERVICES_ROOT.rglob("pom.xml"):
        if SKIP_SEGMENTS & set(pom.relative_to(MICROSERVICES_ROOT).parts):
            continue
        yield pom


def build_graph() -> Tuple[Dict[str, Path], Dict[str, List[str]]]:
    """Return (artifactId -> project_dir, upstream -> [consumers])."""
    project_dirs: Dict[str, Path] = {}
    parents: Dict[str, str] = {}
    deps: Dict[str, Set[str]] = defaultdict(set)

    for pom in _iter_poms():
        root = ET.parse(pom).getroot()
        parent = root.find("m:parent", NS)
        parent_group = _text(parent.find("m:groupId", NS)) if parent is not None else ""
        parent_artifact = _text(parent.find("m:artifactId", NS)) if parent is not None else ""
        group_id = _text(root.find("m:groupId", NS)) or parent_group
        artifact_id = _text(root.find("m:artifactId", NS))

        if group_id != MS_GROUP_ID or not artifact_id:
            continue
        if artifact_id in project_dirs:
            raise SystemExit(
                f"Duplicate artifactId '{artifact_id}' in "
                f"{project_dirs[artifact_id]} and {pom.parent}"
            )
        project_dirs[artifact_id] = pom.parent
        if parent_group == MS_GROUP_ID and parent_artifact:
            parents[artifact_id] = parent_artifact

        for dep in root.iter(f"{{{MAVEN_NS}}}dependency"):
            if _text(dep.find("m:groupId", NS)) != MS_GROUP_ID:
                continue
            dep_art = _text(dep.find("m:artifactId", NS))
            if dep_art:
                deps[artifact_id].add(dep_art)

    consumers: Dict[str, List[str]] = defaultdict(list)
    for child, parent_id in parents.items():
        if parent_id in project_dirs:
            consumers[parent_id].append(child)
    for consumer, dep_ids in deps.items():
        for dep_id in dep_ids:
            if dep_id in project_dirs and consumer not in consumers[dep_id]:
                consumers[dep_id].append(consumer)

    # Deterministic ordering of consumers per upstream.
    for k in consumers:
        consumers[k].sort()
    return project_dirs, consumers


def find_start(project_dirs: Dict[str, Path], starting: str) -> str:
    """Resolve a starting project specified as either an artifactId or a path."""
    if starting in project_dirs:
        return starting
    candidate = (REPO_ROOT / starting.rstrip("/")).resolve()
    for art, d in project_dirs.items():
        if d.resolve() == candidate:
            return art
    raise SystemExit(
        f"Starting project '{starting}' was not found among microservices/ projects."
    )


def cascade(start: str, consumers: Dict[str, List[str]]) -> List[str]:
    """Return start + transitive dependents in topological build order."""
    in_scope: Set[str] = {start}
    queue = deque([start])
    while queue:
        current = queue.popleft()
        for c in consumers.get(current, ()):
            if c not in in_scope:
                in_scope.add(c)
                queue.append(c)

    indegree: Dict[str, int] = {a: 0 for a in in_scope}
    children: Dict[str, List[str]] = {a: [] for a in in_scope}
    for upstream in in_scope:
        for c in consumers.get(upstream, ()):
            if c in in_scope:
                children[upstream].append(c)
                indegree[c] += 1

    ready: List[str] = sorted(a for a, d in indegree.items() if d == 0)
    if start in ready:
        ready.remove(start)
        ready.insert(0, start)

    order: List[str] = []
    while ready:
        node = ready.pop(0)
        order.append(node)
        for child in sorted(children[node]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
        ready.sort()

    if len(order) != len(in_scope):
        cycle = [a for a, d in indegree.items() if d > 0]
        raise SystemExit(f"Cycle detected in microservice graph among: {cycle}")
    return order


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("starting_project")
    parser.add_argument(
        "--compact", action="store_true",
        help="Emit single-line JSON suitable for $GITHUB_OUTPUT.",
    )
    args = parser.parse_args(argv)

    project_dirs, consumers = build_graph()
    start = find_start(project_dirs, args.starting_project)
    ordered = cascade(start, consumers)

    payload = [
        {"artifactId": a, "path": str(project_dirs[a].relative_to(REPO_ROOT))}
        for a in ordered
    ]
    if args.compact:
        json.dump(payload, sys.stdout, separators=(",", ":"))
    else:
        json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
