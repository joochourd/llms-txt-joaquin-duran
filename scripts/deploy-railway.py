#!/usr/bin/env python3
"""Deploy a verified checkout, keeping Railway crawl workers mutually exclusive."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen


class DeploymentError(RuntimeError):
    pass


class RailwayDeployment:
    INACTIVE_STATUSES = {"REMOVED", "FAILED", "SKIPPED"}

    def __init__(self, environment, sleep=time.sleep, clock=time.monotonic):
        self.project = environment["RAILWAY_PROJECT_ID"]
        self.environment = environment["RAILWAY_ENVIRONMENT_ID"]
        self.service = environment["RAILWAY_SERVICE_ID"]
        self.public_url = environment["RAILWAY_PUBLIC_URL"].rstrip("/")
        self.sleep = sleep
        self.clock = clock

    def command(self, *arguments):
        result = subprocess.run(arguments, text=True, capture_output=True)
        if result.returncode:
            # Do not echo provider output: authentication failures can contain sensitive data.
            raise DeploymentError(f"Command failed: {arguments[0]} {arguments[1]}")
        return result.stdout

    def api(self, query, variables=None):
        arguments = ["railway", "api", query, "--compact"]
        if variables is not None:
            arguments.extend(["--variables", json.dumps(variables)])
        response = json.loads(self.command(*arguments))
        if response.get("errors"):
            raise DeploymentError("Railway API rejected the operation")
        return response["data"]

    def deployments(self):
        deployments = json.loads(self.command(
            "railway", "deployment", "list",
            "--project", self.project,
            "--environment", self.environment,
            "--service", self.service,
            "--limit", "1000", "--json",
        ))
        if len(deployments) >= 1000:
            raise DeploymentError("Deployment history requires manual inspection before replacement")
        return deployments

    def validate_checkout(self, expected_commit):
        actual_commit = self.command("git", "rev-parse", "HEAD").strip()
        if actual_commit != expected_commit:
            raise DeploymentError("Checkout does not match the requested release")
        if self.command("git", "status", "--porcelain", "--untracked-files=normal").strip():
            raise DeploymentError("Release checkout contains uncommitted files")
        scope = self.api("query { projectToken { projectId environmentId } }")["projectToken"]
        if scope != {"projectId": self.project, "environmentId": self.environment}:
            raise DeploymentError("Railway token is not scoped to the configured project/environment")

    def stop_previous_deployments(self):
        previous = [deployment for deployment in self.deployments()
                    if deployment["status"] not in self.INACTIVE_STATUSES]
        for deployment in previous:
            print(f"Stopping previous deployment {deployment['id']}", flush=True)
            self.api("mutation($id:String!) { deploymentRemove(id:$id) }", {"id": deployment["id"]})

        deadline = self.clock() + 180
        while True:
            active = [deployment for deployment in self.deployments()
                      if deployment["status"] not in self.INACTIVE_STATUSES]
            if not active:
                return
            if self.clock() >= deadline:
                raise DeploymentError("Previous deployments are still active; replacement was not started")
            self.sleep(5)

    def upload(self, commit):
        response = self.command(
            "railway", "up",
            "--project", self.project,
            "--environment", self.environment,
            "--service", self.service,
            "--detach", "--json", "--message", f"release: {commit}",
        )
        # Detached CLI output is newline-delimited JSON, including upload progress.
        for line in response.splitlines():
            event = json.loads(line)
            if event.get("deploymentId"):
                return event["deploymentId"]
        raise DeploymentError("Railway did not return a deployment ID")

    def wait_for_deployment(self, deployment_id):
        deadline = self.clock() + 900
        previous_status = None
        while self.clock() < deadline:
            deployment = next((item for item in self.deployments() if item["id"] == deployment_id), None)
            if deployment:
                status = deployment["status"]
                if status != previous_status:
                    print(f"Deployment {deployment_id}: {status}", flush=True)
                    previous_status = status
                if status == "SUCCESS":
                    return
                if status in self.INACTIVE_STATUSES | {"CRASHED"}:
                    raise DeploymentError(f"Replacement deployment ended with {status}")
            self.sleep(10)
        raise DeploymentError("Replacement did not become healthy before the deployment timeout")

    def health_is_up(self, path):
        try:
            with urlopen(self.public_url + path, timeout=15) as response:
                return response.status == 200 and json.load(response).get("status") == "UP"
        except (URLError, TimeoutError, ValueError):
            return False

    def verify_public_health(self):
        deadline = self.clock() + 120
        while self.clock() < deadline:
            if self.health_is_up("/health/liveness") and self.health_is_up("/health/readiness"):
                return
            self.sleep(5)
        raise DeploymentError("Public liveness/readiness checks failed")

    def deploy(self, commit):
        self.validate_checkout(commit)
        self.stop_previous_deployments()
        deployment_id = self.upload(commit)
        self.wait_for_deployment(deployment_id)
        self.verify_public_health()
        summary = f"Deployed `{commit}` to {self.public_url}\n\nRailway deployment: `{deployment_id}`\n"
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
                output.write(summary)
        print(summary, flush=True)


def main():
    try:
        if len(sys.argv) != 2:
            raise DeploymentError("Usage: deploy-railway.py EXPECTED_COMMIT_SHA")
        if not os.environ.get("RAILWAY_TOKEN"):
            raise DeploymentError("A Railway project token is required")
        RailwayDeployment(os.environ).deploy(sys.argv[1])
    except (DeploymentError, KeyError) as error:
        print(f"Deployment failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
