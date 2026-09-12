# kafka-lambda-oauth-java-sam / iam_auth
# Java AWS Lambda consumer for Amazon MSK via a self-managed Kafka event source with IAM_AUTH

> **Under development.** This variant exercises the AWS Lambda **`IAM_AUTH`** auth type for a **self-managed** Kafka event source, which is not yet generally available. The fully-verified reference variant is [`oauthbearer_auth`](../oauthbearer_auth).

This pattern is a Lambda function that consumes from an **Amazon MSK cluster with IAM authentication** — but instead of using the native MSK event source, the cluster is declared to Lambda as a **self-managed** Kafka event source pointed at its **IAM bootstrap endpoint (`:9098`)** with the **`IAM_AUTH`** auth type (the "Kafka OAuth & IAM Testing Manual", Route B). The Lambda function parses each Kafka message and writes it (fields + Kafka metadata) to Amazon DynamoDB.

## Why MSK (not the 3 EC2 brokers)
`IAM_AUTH` is `AWS_MSK_IAM` — a server-side capability of Amazon MSK. Self-managed brokers on EC2 cannot validate it, so this variant provisions an **MSK cluster** (the sibling variants use self-managed EC2 brokers). Authorization is by **IAM policy** (`kafka-cluster:*` actions), not Kafka ACLs.

## Files
- `MSKAndClientEC2.yaml` - CloudFormation: MSK (IAM) cluster, a client EC2 instance, and three IAM client roles (admin/producer/consumer).
- `kafka_event_consumer_function/` - the Java consumer (writes to DynamoDB).
- `kafka_json_apps/` - Datafaker JSON producer/consumer (built with `aws-msk-iam-auth`).
- `scripts/` - `refresh_token.sh` (writes an AWS_MSK_IAM client.properties per role), `admin_create_topic.sh`, `producer_send.sh`, `consumer_receive.sh`, and negative tests.
- `deploy_lambda_iam_cli.sh` - deploys the Lambda + `IAM_AUTH` event source via the AWS CLI.
- `template_original.yaml` - SAM template (function + DynamoDB table); SAM does not support this auth type, so use the CLI deploy.

## Identity & authorization model
No Cognito, no Kafka ACLs. MSK IAM authorization is enforced by IAM policies attached to each role; the client selects its role via `awsRoleArn` in the JAAS config:

| Role | IAM role (default) | kafka-cluster permissions |
|---|---|---|
| Admin | `<stack>-kafka-admin` | `*` (create topics, read/write, groups) |
| Producer | `<stack>-kafka-producer` | Connect, DescribeTopic, WriteData |
| Consumer | `<stack>-kafka-consumer` | Connect, DescribeTopic, ReadData, Describe/AlterGroup |
| Lambda poller | the function's execution role | Connect, DescribeTopic, ReadData, Describe/AlterGroup |

Each client's `client.properties` uses:
```properties
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="<role arn>" awsStsRegion="<region>";
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```
MSK presents a publicly-trusted TLS certificate, so no truststore / `SERVER_ROOT_CA_CERTIFICATE` is needed.

## Deploy
1. Deploy `MSKAndClientEC2.yaml` (CloudFormation). MSK cluster creation takes ~20-30 minutes. Wait for `CREATE_COMPLETE`, then a few minutes for the client UserData.
2. Connect to the client EC2 (`KafkaClientInstance`) via EC2 Instance Connect. The client already created the topic (`cat topic_creator_output.txt`).
3. Deploy the Lambda + event source mapping:
   ```bash
   export AWS_REGION=us-west-2
   cd ~/serverless-patterns/kafka-lambda-oauth-java-sam/iam_auth
   bash deploy_lambda_iam_cli.sh
   ```
   It resolves the IAM bootstrap brokers, creates the DynamoDB table (`KafkaIamAuth`), grants the execution role `kafka-cluster` read access + `dynamodb:PutItem`, and creates the ESM with `{Type: IAM_AUTH}`.

## Test
```bash
aws lambda list-event-source-mappings --function-name kafka-iam-consumer \
  --query 'EventSourceMappings[].[UUID,State,LastProcessingResult]' --output table
bash scripts/producer_send.sh $KAFKA_TOPIC 10
aws dynamodb scan --table-name KafkaIamAuth --max-items 5
```
Negative tests: `bash scripts/bad_invalid_credentials.sh` and `bash scripts/bad_unauthorized_operations.sh`.

## Cleanup
Delete the event source mapping and function, the DynamoDB table (`KafkaIamAuth`), and the execution role; then delete the CloudFormation stack (deleting the MSK cluster takes a while).
