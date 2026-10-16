# Deployment

The deployment target is one always-on Railway application JVM and one regional AWS DynamoDB table. Provisioning, account authentication and deployment are separate administrative steps.

## Local readiness checks

Run `./scripts/verify.sh` and the frontend browser tests documented in the README. These use DynamoDB Local, not AWS. Validate `infra/dynamodb.json` with `cfn-lint -t infra/dynamodb.json`; `railway.json` can be checked against its declared provider JSON schema. Local success does not establish AWS latency, IAM correctness or Railway ingress behavior.

## AWS table and administrative bootstrap

Use a separate provisioning identity, preferably a temporary AWS CLI session. Keep its profile outside the repository. Select `us-east-2` (Ohio) and place the application in the closest available Railway region.

The CloudFormation template creates exactly one PAY_PER_REQUEST DynamoDB table with string `pk`/`sk`, KEYS_ONLY `WorkIndex` (`workPk`/`workSk`) and `DueIndex` (`duePk`/`dueSk`), TTL on `expiresAtEpochSeconds`, and evaluation tags. Default AWS-owned encryption applies. No IAM principal, access key, stream, alarm, extra database or network resource is created. PITR and deletion protection are not enabled for this disposable evaluation deployment. Deleting the stack deletes the table and its data.

After explicitly authorizing resource creation:

```sh
aws cloudformation deploy \
  --profile profound-provisioning \
  --region us-east-2 \
  --stack-name llms-txt-evaluation \
  --template-file infra/dynamodb.json \
  --parameter-overrides TableName=llms-txt-production
```

Wait for the table and both indexes to be ACTIVE. Initialize metadata with administrative credentials, before starting application workers:

```sh
AWS_PROFILE=profound-provisioning APP_ENV=aws AWS_REGION=us-east-2 \
DYNAMODB_TABLE=llms-txt-production ./gradlew run --args=--init-aws
```

Ensure `DYNAMODB_ENDPOINT` is unset. The command runs without starting the server or background workers. It atomically creates only `CAPACITY#GLOBAL / META` and `CONFIG#GLOBAL / META`, with conditional puts and a stable SDK transaction token. Compatible initialized metadata is accepted without changes, including nonzero counters. Partial/incompatible initialization is rejected for manual inspection. If both records are missing, an administrative, consistent, one-item Scan rejects occupied tables instead of recreating counters over existing data. A lost transaction response is reconciled from an atomic metadata read.

Bootstrap requires table-scoped DescribeTable, GetItem, PutItem and Scan permissions. The provisioning identity additionally needs the control-plane permissions for its reviewed CloudFormation stack. These administrative permissions are never assigned to the running application. The separate `--init-local` command still refuses AWS.

## Runtime credentials

Render the three nonsecret placeholders in `infra/runtime-policy.json`: `AWS_REGION`, `AWS_ACCOUNT_ID` and `DYNAMODB_TABLE`. The resulting policy grants only the table data operations used by the application and Query on its two exact index ARNs. Transaction actions use the underlying GetItem/PutItem/UpdateItem/DeleteItem/ConditionCheckItem permissions. It grants no Scan, table creation/deletion, IAM administration or wildcard resource access.

Use a dedicated application identity. If using access keys, enter them directly as sealed Railway service variables and rotate/revoke them after evaluation. Never use root or provisioning credentials at runtime. Temporary session credentials additionally require `AWS_SESSION_TOKEN` and a renewal mechanism; manually supplying an expiring token is not a durable runtime authentication solution. No credentials belong in Git, Docker build arguments, frontend variables or logs. AWS SSO/assume-role profiles are supported by the SDK modules for administrative commands.

## Railway service

Use the repository Dockerfile and `railway.json`. It sets one replica, readiness at `/health/readiness`, a 180-second startup health timeout, five restart attempts on failure, zero post-activation overlap and ten seconds for draining. Preserve one region/one replica in the dashboard. Disable Serverless/app sleeping so monitoring continues without visitors. Set a resource ceiling and usage alert before exposing the application; allow process memory above the 384 MiB Java heap and measure peak RSS under representative load.

Use `.env.production.example` as a nonsecret configuration reference; enter values in the service settings. Supply runtime credentials separately as sealed variables. Bind to Railway's injected `PORT`. Do not set `DYNAMODB_ENDPOINT`. Keep normal monitoring intervals, quotas, source-body limits and request/deadline budgets. Current frontend/API URLs are same-origin and server-relative, so `PUBLIC_BASE_URL` and CORS overrides are unnecessary. The accelerated monitoring source is test-only; there is no public demo-enable variable.

### Public API behavior

The API does not use client-IP identification, forwarded-header trust, HTTP rate limits or an open-batch quota. No trusted-proxy CIDRs are required. Each submission is bounded to 1,000 inputs and the configured body/URL sizes. The API executor, crawl workers, outstanding-run capacity and automatic-monitoring quota remain bounded. Accepted inputs wait durably for crawl capacity.

### Updates and single-JVM operation

Keep Railway's native GitHub autodeploy disabled. GitHub Actions owns production releases. Railway's zero overlap setting applies after the new deployment is active; the new JVM can start workers while its readiness check runs. It alone does not guarantee process exclusivity.

The `Deploy production` GitHub Actions workflow verifies pull requests to `develop` and `main`. Every push to `main` verifies that exact commit, then deploys it with the pinned Railway CLI. Releases are serialized without cancelling an active release. The deployment helper removes previous deployments and confirms they are stopped before uploading the replacement, waits for Railway success, then checks public liveness and readiness. A stop timeout prevents the replacement from starting. A failed build/startup leaves production stopped and requires a corrected release or rollback; it never starts a second JVM to hide the failure. Brief downtime is intentional.

Configure these repository Actions variables: `RAILWAY_PROJECT_ID`, `RAILWAY_ENVIRONMENT_ID`, `RAILWAY_SERVICE_ID` and `RAILWAY_PUBLIC_URL`. Store only a Railway project/environment-scoped token as the Actions secret `RAILWAY_TOKEN`. AWS credentials remain sealed service variables in Railway; GitHub Actions does not receive them. Never substitute a workspace/account token. Restrict repository write access because main workflows can use the deployment token.

Develop changes through pull requests to `develop`, then merge a release pull request from `develop` to `main`. The workflow summary records the commit, deployment ID and public URL. Use **Run workflow** on `main` to retry the current release after resolving an operational failure. To roll back, revert the release through the same pull-request flow so the reviewed rollback is also the version tracked by `main`. Releases that require the removed batch-capacity counter and reservation fields need a data migration before reusing this table. No filesystem volume is needed because durable application state lives in DynamoDB. Do not run a local AWS-connected worker alongside the deployed service.

## Public verification

- Check liveness and readiness; readiness shares storage probes for at most two seconds and remains independent of crawl capacity.
- Submit a small batch and a larger bounded batch; measure 202 latency separately from queue waiting and crawl duration.
- Verify per-row completion/failures, previews, individual downloads and ZIP-page hashes.
- Repeat inputs for cached/shared-work behavior; force consecutive refreshes after each previous run finishes.
- Enable/disable monitoring and demonstrate a real automatic update using a controlled source.
- Restart the application and verify durable waiting work and accepted files survive.
- Simulate a failed source update and verify the accepted file remains downloadable.
- Check representative real-site output quality, memory, API latency and DynamoDB consumption. Local fixture throughput is not AWS throughput.

Add the verified HTTPS URL and screenshots/video to the README after deployment. Configure a simple AWS Budget alert administratively; an alert is not a hard spending cap. Keep existing application quotas and retire resources after evaluation.

## Rollback and cleanup

Roll back using the same stop-before-start procedure and a previously verified application version. Preserve the table unless intentionally removing the evaluation. To withdraw public access, remove the Railway public domain or stop the service. Stop workers before deleting storage.

Delete the CloudFormation stack to remove the evaluation table, revoke the dedicated runtime credentials, remove its IAM identity/policy, and remove the Railway service/project. Check both provider inventories and billing dashboards for leftovers, including failed replacement stacks or retained resources. Preserve any desired results before deletion.

Provider references: [Railway configuration](https://docs.railway.com/config-as-code/reference), [deployment teardown](https://docs.railway.com/deployments/deployment-teardown), [sealed variables](https://docs.railway.com/variables#sealed-variables), [DynamoDB transaction IAM](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis-iam.html).
