import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock


module_spec = importlib.util.spec_from_file_location("deploy", Path(__file__).parents[1] / "deploy-railway.py")
deploy = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(deploy)


class RailwayDeploymentTest(unittest.TestCase):
    def setUp(self):
        self.release = deploy.RailwayDeployment({
            "RAILWAY_PROJECT_ID": "project-id",
            "RAILWAY_ENVIRONMENT_ID": "environment-id",
            "RAILWAY_SERVICE_ID": "service-id",
            "RAILWAY_PUBLIC_URL": "https://example.com",
        }, sleep=Mock())

    def test_replacement_waits_until_previous_workers_are_removed(self):
        events = []
        self.release.validate_checkout = Mock()
        self.release.api = Mock(side_effect=lambda *args: events.append("remove"))
        self.release.deployments = Mock(side_effect=[
            [{"id": "previous", "status": "SUCCESS"}],
            [{"id": "previous", "status": "REMOVING"}],
            [{"id": "previous", "status": "REMOVED"}],
        ])
        self.release.upload = Mock(side_effect=lambda commit: events.append("upload") or "replacement")
        self.release.wait_for_deployment = Mock()
        self.release.verify_public_health = Mock()

        self.release.deploy("release-commit")

        self.assertEqual(["remove", "upload"], events)
        self.assertEqual(3, self.release.deployments.call_count)
        self.release.sleep.assert_called_once_with(5)

    def test_stop_timeout_does_not_start_a_replacement(self):
        self.release.validate_checkout = Mock()
        self.release.api = Mock()
        self.release.deployments = Mock(return_value=[{"id": "previous", "status": "REMOVING"}])
        self.release.clock = Mock(side_effect=[0, 181])
        self.release.upload = Mock()

        with self.assertRaisesRegex(deploy.DeploymentError, "still active"):
            self.release.deploy("release-commit")

        self.release.upload.assert_not_called()

    def test_wrong_checkout_does_not_stop_existing_service(self):
        self.release.command = Mock(return_value="different-commit")
        self.release.stop_previous_deployments = Mock()

        with self.assertRaisesRegex(deploy.DeploymentError, "does not match"):
            self.release.deploy("release-commit")

        self.release.stop_previous_deployments.assert_not_called()

    def test_wrong_token_scope_does_not_stop_existing_service(self):
        self.release.command = Mock(side_effect=["release-commit", ""])
        self.release.api = Mock(return_value={"projectToken": {
            "projectId": "another-project", "environmentId": "environment-id",
        }})
        self.release.stop_previous_deployments = Mock()

        with self.assertRaisesRegex(deploy.DeploymentError, "not scoped"):
            self.release.deploy("release-commit")

        self.release.stop_previous_deployments.assert_not_called()

    def test_failed_replacement_is_not_reported_as_successful(self):
        self.release.deployments = Mock(return_value=[{"id": "replacement", "status": "FAILED"}])

        with self.assertRaisesRegex(deploy.DeploymentError, "FAILED"):
            self.release.wait_for_deployment("replacement")

    def test_private_health_is_not_sufficient_without_public_health(self):
        self.release.clock = Mock(side_effect=[0, 1, 121])
        self.release.health_is_up = Mock(return_value=False)

        with self.assertRaisesRegex(deploy.DeploymentError, "Public liveness/readiness"):
            self.release.verify_public_health()

    def test_upload_reads_deployment_id_from_progress_events(self):
        self.release.command = Mock(return_value='{"message":"Uploading"}\n{"deploymentId":"replacement"}\n')

        self.assertEqual("replacement", self.release.upload("release-commit"))


if __name__ == "__main__":
    unittest.main()
