# kafka-lambda-oauth-java-sam / iam_oauthbearer_auth
# Java AWS Lambda consumer for a self-managed Apache Kafka cluster with IAM Outbound (SASL/OAUTHBEARER) authentication

> **Under development.** This variant exercises the AWS Lambda self-managed Kafka **`IAM_OAUTHBEARER_AUTH`** ("IAM Outbound") event-source auth type, which is not yet generally available. Some pieces (the `sts:GetWebIdentityToken` CLI shape, the token claims, and the AWS STS OIDC issuer discovery) are best-effort scaffolding and may need adjustment as the feature is finalized. The sibling [`oauthbearer_auth`](../oauthbearer_auth) variant (Cognito) is the fully-verified reference.

This pattern is a Lambda function that consumes from a **self-managed Apache Kafka** cluster (3 brokers on EC2, KRaft) that authenticates clients with **SASL/OAUTHBEARER** — but the tokens are **AWS IAM Outbound Identity Federation web-identity (OIDC) tokens** rather than tokens from an external IdP. There is **no Cognito / Keycloak** and **no client secret**: every identity is an AWS IAM role, federated outward as an OIDC token that the brokers validate against the **AWS STS OIDC JWKS** endpoint. The Lambda function parses each Kafka message and writes it (fields + Kafka metadata) to Amazon DynamoDB.

## What "IAM Outbound" is
AWS mints a signed OIDC JWT from an IAM role via `sts:GetWebIdentityToken`. The token's issuer is the account's AWS STS Outbound Identity Federation issuer:

- **Issuer**: `https://<uuid>.tokens.sts.global.api.aws`
- **JWKS**: `https://<uuid>.tokens.sts.global.api.aws/.well-known/jwks.json`
- **Audience**: required (`kafka-cluster` here)

The `<uuid>` is account-specific — enable federation (`aws iam enable-outbound-web-identity-federation`) then read it with `aws iam get-outbound-web-identity-federation-info`. The brokers trust that issuer/JWKS and require the audience. Compared to the Cognito variant: no IdP to run, no secret to store; audience checking is **enabled** (Cognito had none).

## Files
- `kafka_event_consumer_function/` - the Java consumer (writes to DynamoDB).
- `kafka_json_apps/` - producer/consumer sample apps.
- `scripts/` - `refresh_token.sh` (mints a web-identity token per role), `admin_create_topic.sh`, `producer_send.sh`, `consumer_receive.sh`, and negative tests.
- `template_original.yaml` - SAM template (function + DynamoDB table). SAM does not support this auth type, so use the CLI deploy below.
- `deploy_lambda_oauth_cli.sh` - deploys the Lambda + `IAM_OAUTHBEARER_AUTH` event source via the AWS CLI.
- `KafkaBrokersClientEC2.yaml` - CloudFormation: the 3-broker cluster, three IAM client roles, and the client EC2 machine.

## Identity & authorization model
No Cognito. Four AWS IAM identities, each federated to a distinct Kafka principal (the token `sub`):

| Role | IAM role (default) | Allowed |
|---|---|---|
| Admin | `<stack>-kafka-admin` | topic/ACL management (via the brokers' internal PLAINTEXT listener, where `ANONYMOUS` is a super user) |
| Producer | `<stack>-kafka-producer` | `WRITE` to the topic |
| Consumer | `<stack>-kafka-consumer` | `READ` from the topic + consumer group |
| Lambda poller | the function's execution role | consumes via the event source |

Each interactive client **assumes** its role and mints a web-identity token (`refresh_token.sh <role>`). Because the token subject is only known after minting, `admin_create_topic.sh` bootstraps the topic + ACLs over the brokers' **internal PLAINTEXT listener** (no token needed) and derives the producer/consumer principals at runtime by decoding the token `sub`.

## Deploy
1. Deploy `KafkaBrokersClientEC2.yaml` (CloudFormation). Optionally set `OutboundIssuerUrl` (leave blank to auto-enable federation + look it up at broker boot) and `OutboundAudience` (default `kafka-cluster`). Wait for `CREATE_COMPLETE` + a few minutes for the brokers.
2. Connect to the client EC2 (`KafkaClientInstance`) via EC2 Instance Connect.
3. Deploy the Lambda + event source mapping:
   ```bash
   export AWS_REGION=us-west-2
   cd ~/serverless-patterns/kafka-lambda-oauth-java-sam/iam_oauthbearer_auth
   bash deploy_lambda_oauth_cli.sh
   ```
   It enables outbound federation, creates the DynamoDB table (`KafkaIamOAuthBearerAuth`), grants the execution role `sts:GetWebIdentityToken`, and creates the ESM with `IAM_OAUTHBEARER_AUTH` + `OAUTHBEARER_AUDIENCE` + `SERVER_ROOT_CA_CERTIFICATE`.

## Test
```bash
aws lambda list-event-source-mappings --function-name kafka-iam-oauth-consumer \
  --query 'EventSourceMappings[].[UUID,State,LastProcessingResult]' --output table
bash scripts/producer_send.sh $KAFKA_TOPIC 10
aws dynamodb scan --table-name KafkaIamOAuthBearerAuth --max-items 5
```
Negative tests: `bash scripts/bad_invalid_credentials.sh` and `bash scripts/bad_unauthorized_operations.sh`.

## Cleanup
Delete the event source mapping and function, the DynamoDB table (`KafkaIamOAuthBearerAuth`), the broker-CA secret, and the execution role; then delete the CloudFormation stack. Optionally remove the S3 Kafka/cert cache bucket (`kafka-*-cache-<account>-<region>`).
