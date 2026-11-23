"""Tests for combining cascade roots and validating project selections."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "cascade_plan", Path(__file__).with_name("cascade-plan.py")
)
planner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(planner)


class CascadeManyTest(unittest.TestCase):
    def test_shared_consumers_are_released_once_after_both_producers(self):
        consumers = {
            "api-a": ["service"],
            "api-b": ["service"],
            "service": ["application"],
        }
        self.assertEqual(
            ["api-a", "api-b", "service", "application"],
            planner.cascade_many(["api-b", "api-a", "api-a"], consumers),
        )

    def test_selected_consumer_waits_for_indirect_selected_producer(self):
        consumers = {"z-api": ["messaging"], "messaging": ["a-service"]}
        self.assertEqual(
            ["z-api", "messaging", "a-service"],
            planner.cascade_many(["a-service", "z-api"], consumers),
        )

    def test_single_start_interface_is_preserved(self):
        self.assertEqual(
            ["api", "service"],
            planner.cascade("api", {"api": ["service"]}),
        )

    def test_cycle_reachable_from_any_start_is_rejected(self):
        consumers = {"api": ["service"], "service": ["api"]}
        with self.assertRaisesRegex(SystemExit, "Cycle detected"):
            planner.cascade_many(["independent", "api"], consumers)

    def test_unrelated_projects_and_cycles_are_not_included(self):
        consumers = {"api": ["service"], "other": ["other"]}
        self.assertEqual(
            ["api", "service"], planner.cascade_many(["api"], consumers)
        )


class ProjectSelectionTest(unittest.TestCase):
    def setUp(self):
        self.projects = {
            name: planner.REPO_ROOT / "microservices" / name
            for name in ["api-a", "api-b", "service", "aggregator", "child"]
        }
        self.consumers = {
            "api-a": ["service"],
            "api-b": ["service"],
            "aggregator": ["child"],
        }

    def run_planner(self, *selections):
        output = io.StringIO()
        with patch.object(
            planner, "build_graph",
            return_value=(self.projects, self.consumers, {"aggregator"}),
        ), contextlib.redirect_stdout(output):
            self.assertEqual(0, planner.main(["--compact", "--", *selections]))
        return json.loads(output.getvalue())

    def test_paths_artifact_ids_commas_and_whitespace_can_be_combined(self):
        result = self.run_planner(
            "microservices/api-b/,\napi-a api-a", "api-b"
        )
        self.assertEqual(["api-a", "api-b", "service"], [p["artifactId"] for p in result])
        self.assertEqual("microservices/api-a", result[0]["path"])

    def test_aggregator_is_skipped_but_its_consumers_are_kept(self):
        result = self.run_planner("aggregator")
        self.assertEqual(["child"], [p["artifactId"] for p in result])

    def test_invalid_selection_aborts_entire_plan_without_json(self):
        output = io.StringIO()
        with patch.object(
            planner, "build_graph",
            return_value=(self.projects, self.consumers, set()),
        ), contextlib.redirect_stdout(output):
            with self.assertRaisesRegex(SystemExit, "missing.*was not found"):
                planner.main(["api-a,missing"])
        self.assertEqual("", output.getvalue())

    def test_empty_selection_is_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                planner.main([" , \n "])
        self.assertEqual(2, error.exception.code)

    def test_empty_release_plan_is_rejected(self):
        with patch.object(
            planner, "build_graph",
            return_value=(self.projects, {}, {"aggregator"}),
        ), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                planner.main(["aggregator"])
        self.assertEqual(2, error.exception.code)


if __name__ == "__main__":
    unittest.main()
