#!/bin/bash
# =============================================================================
# Deploy the Java Kafka consumer as a Lambda function with a SELF-MANAGED KAFKA
# event source using SASL/OAUTHBEARER (OAuth 2.0 client-credentials) auth, via
# the AWS CLI.
#
# AWS SAM does not yet support the OAUTHBEARER auth type for self-managed Kafka
# event sources, so this script uses `aws lambda create-event-source-mapping`
# directly (per the "Kafka OAuth & IAM Testing Manual").
#
# It reuses the same Java consumer as the SAM path
# (com.amazonaws.services.lambda.samples.events.msk.HandlerMSK).
#
# -----------------------------------------------------------------------------
# PREREQUISITES (these are NOT created by this script and are assumed to exist):
#   1. Your AWS account is ALLOWLISTED for the new self-managed Kafka ESM auth
#      types (OAUTHBEARER_AUTH). Otherwise create-event-source-mapping fails.
#   2. The brokers expose a SASL_SSL / OAUTHBEARER listener (e.g. :9093) with a
#      TLS server certificate. The Lambda poller connects over TLS and validates
#      the broker cert against SERVER_ROOT_CA_CERTIFICATE. (The pattern's default
#      cluster listens SASL_PLAINTEXT on :9092 - add a TLS listener first.)
#   3. A Cognito app client with the client_credentials grant, a user-pool
#      domain, and a resource server + scope (e.g. kafka/consume). The broker's
#      JWKS validator must accept these tokens (no audience is present).
#
# This script CAN create the two Secrets Manager secrets for you (see CREATE
# secret sections) if you pass the raw values instead of pre-made ARNs.
# =============================================================================
set -euo pipefail

# ---------------------------- Configuration ---------------------------------
REGION="${AWS_REGION:-us-west-2}"
STACK_NAME="${STACK_NAME:-kafka-oauth}"                    # CFN stack for VPC/subnet/SG discovery
FUNCTION_NAME="${FUNCTION_NAME:-kafka-oauth-selfmanaged-consumer}"
RUNTIME="${RUNTIME:-java21}"
HANDLER="com.amazonaws.services.lambda.samples.events.msk.HandlerMSK::handleRequest"
JAR_PATH="${JAR_PATH:-kafka_event_consumer_function/target/MSKConsumer-1.0.jar}"
TOPIC="${TOPIC:-KafkaOAuthJavaLambdaTopic}"
CONSUMER_GROUP="${CONSUMER_GROUP:-lambda-oauth-consumer}"
POLLER_GROUP="${POLLER_GROUP:-${CONSUMER_GROUP}-cell1}"    # must be a fresh name per VPC + event source type
BATCH_SIZE="${BATCH_SIZE:-10}"

# Bootstrap servers MUST be the SASL_SSL / OAUTHBEARER listener (TLS), e.g. :9093.
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-10.0.1.10:9093,10.0.2.10:9093,10.0.3.10:9093}"

# --- OAuth client-credentials secret: pass an existing ARN, OR the raw values ---
OAUTH_SECRET_ARN="${OAUTH_SECRET_ARN:-}"
OAUTH_CLIENT_ID="${OAUTH_CLIENT_ID:-}"
OAUTH_CLIENT_SECRET="${OAUTH_CLIENT_SECRET:-}"
OAUTH_TOKEN_ENDPOINT="${OAUTH_TOKEN_ENDPOINT:-}"          # https://<domain>.auth.<region>.amazoncognito.com/oauth2/token
OAUTH_SCOPE="${OAUTH_SCOPE:-kafka/consume}"

# --- Broker CA trust anchor secret: pass an existing ARN, OR a PEM file path ---
SERVER_CA_SECRET_ARN="${SERVER_CA_SECRET_ARN:-}"
BROKER_CA_CERT_FILE="${BROKER_CA_CERT_FILE:-}"            # PEM file of the broker's CA / self-signed cert

# ---------------------- Discover VPC config from the stack -------------------
get_output() {
  aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text 2>/dev/null
}
SUBNET1="${SUBNET1:-$(get_output PrivateSubnetOne)}"
SUBNET2="${SUBNET2:-$(get_output PrivateSubnetTwo)}"
SUBNET3="${SUBNET3:-$(get_output PrivateSubnetThree)}"
SG_ID="${SG_ID:-$(get_output KafkaBrokerSecurityGroupId)}"
[ -n "$SUBNET1" ] && [ -n "$SG_ID" ] || { echo "ERROR: could not resolve subnets/SG from stack $STACK_NAME; set SUBNET1..3 and SG_ID"; exit 1; }

# ------------------------- Create secrets if needed --------------------------
if [ -z "$OAUTH_SECRET_ARN" ]; then
  [ -n "$OAUTH_CLIENT_ID" ] && [ -n "$OAUTH_CLIENT_SECRET" ] && [ -n "$OAUTH_TOKEN_ENDPOINT" ] || {
    echo "ERROR: set OAUTH_SECRET_ARN, or OAUTH_CLIENT_ID + OAUTH_CLIENT_SECRET + OAUTH_TOKEN_ENDPOINT to create it"; exit 1; }
  echo "Creating OAuth client-credentials secret..."
  OAUTH_SECRET_ARN=$(aws secretsmanager create-secret --region "$REGION" \
    --name "${FUNCTION_NAME}-oauth-creds" \
    --secret-string "{\"oauthClientId\":\"$OAUTH_CLIENT_ID\",\"oauthClientSecret\":\"$OAUTH_CLIENT_SECRET\",\"oauthTokenEndpointUrl\":\"$OAUTH_TOKEN_ENDPOINT\"}" \
    --query 'ARN' --output text)
fi

if [ -z "$SERVER_CA_SECRET_ARN" ]; then
  [ -n "$BROKER_CA_CERT_FILE" ] && [ -f "$BROKER_CA_CERT_FILE" ] || {
    echo "ERROR: set SERVER_CA_SECRET_ARN, or BROKER_CA_CERT_FILE (PEM) to create it"; exit 1; }
  echo "Creating broker CA trust-anchor secret (field 'certificate')..."
  SERVER_CA_SECRET_ARN=$(aws secretsmanager create-secret --region "$REGION" \
    --name "${FUNCTION_NAME}-broker-ca" \
    --secret-string "$(jq -n --arg c "$(cat "$BROKER_CA_CERT_FILE")" '{certificate:$c}')" \
    --query 'ARN' --output text)
fi

# ------------------------------ Build the jar --------------------------------
if [ ! -f "$JAR_PATH" ]; then
  echo "Building consumer jar..."
  ( cd "$(dirname "$0")/kafka_event_consumer_function" && mvn -q -DskipTests package )
fi

# ---------------------------- Execution role ---------------------------------
ROLE_NAME="${ROLE_NAME:-${FUNCTION_NAME}-role}"
if ! aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "Creating execution role $ROLE_NAME..."
  aws iam create-role --role-name "$ROLE_NAME" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam attach-role-policy --role-name "$ROLE_NAME" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  # Self-managed Kafka ESM: the poller creates ENIs in your VPC and reads the
  # OAuth + CA secrets on the execution role's behalf.
  aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name kafka-esm-access --policy-document "{
    \"Version\":\"2012-10-17\",\"Statement\":[
      {\"Effect\":\"Allow\",\"Action\":[\"ec2:CreateNetworkInterface\",\"ec2:DescribeNetworkInterfaces\",\"ec2:DeleteNetworkInterface\",\"ec2:DescribeSecurityGroups\",\"ec2:DescribeSubnets\",\"ec2:DescribeVpcs\"],\"Resource\":\"*\"},
      {\"Effect\":\"Allow\",\"Action\":[\"secretsmanager:GetSecretValue\"],\"Resource\":[\"$OAUTH_SECRET_ARN\",\"$SERVER_CA_SECRET_ARN\"]}
    ]}"
  echo "Waiting for role to propagate..."; sleep 15
fi
ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)

# ------------------------- Create/update the function ------------------------
if aws lambda get-function --region "$REGION" --function-name "$FUNCTION_NAME" >/dev/null 2>&1; then
  echo "Updating function code..."
  aws lambda update-function-code --region "$REGION" --function-name "$FUNCTION_NAME" \
    --zip-file "fileb://$JAR_PATH" >/dev/null
else
  echo "Creating function $FUNCTION_NAME..."
  aws lambda create-function --region "$REGION" --function-name "$FUNCTION_NAME" \
    --runtime "$RUNTIME" --role "$ROLE_ARN" --handler "$HANDLER" \
    --zip-file "fileb://$JAR_PATH" --timeout 60 --memory-size 512 >/dev/null
fi
aws lambda wait function-active-v2 --region "$REGION" --function-name "$FUNCTION_NAME" 2>/dev/null || sleep 10

# --------- Build the bootstrap-servers JSON array from the CSV list ----------
BS_JSON=$(python3 -c "import json,sys;print(json.dumps([s for s in sys.argv[1].split(',') if s]))" "$BOOTSTRAP_SERVERS")

# ------------------------- Create the event source mapping -------------------
echo "Creating self-managed Kafka event source mapping (OAUTHBEARER)..."
aws lambda create-event-source-mapping --region "$REGION" \
  --function-name "$FUNCTION_NAME" \
  --topics "$TOPIC" \
  --self-managed-event-source "{\"Endpoints\":{\"KAFKA_BOOTSTRAP_SERVERS\":$BS_JSON}}" \
  --self-managed-kafka-event-source-config "{\"ConsumerGroupId\":\"$CONSUMER_GROUP\"}" \
  --source-access-configurations "[
    {\"Type\":\"VPC_SUBNET\",\"URI\":\"subnet:$SUBNET1\"},
    {\"Type\":\"VPC_SUBNET\",\"URI\":\"subnet:$SUBNET2\"},
    {\"Type\":\"VPC_SUBNET\",\"URI\":\"subnet:$SUBNET3\"},
    {\"Type\":\"VPC_SECURITY_GROUP\",\"URI\":\"security_group:$SG_ID\"},
    {\"Type\":\"OAUTHBEARER_AUTH\",\"URI\":\"$OAUTH_SECRET_ARN\"},
    {\"Type\":\"OAUTHBEARER_SCOPE\",\"URI\":\"$OAUTH_SCOPE\"},
    {\"Type\":\"SERVER_ROOT_CA_CERTIFICATE\",\"URI\":\"$SERVER_CA_SECRET_ARN\"}
  ]" \
  --provisioned-poller-config "{\"PollerGroupName\":\"$POLLER_GROUP\",\"MinimumPollers\":1,\"MaximumPollers\":1}" \
  --starting-position TRIM_HORIZON \
  --batch-size "$BATCH_SIZE"

echo
echo "Done. Watch the mapping reach Enabled and process records:"
echo "  aws lambda list-event-source-mappings --region $REGION --function-name $FUNCTION_NAME \\"
echo "    --query 'EventSourceMappings[].[UUID,State,LastProcessingResult]' --output table"
echo "Then produce messages (scripts/producer_send.sh $TOPIC 10) and check the function's CloudWatch logs."
