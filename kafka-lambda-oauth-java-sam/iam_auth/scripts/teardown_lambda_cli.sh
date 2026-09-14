#!/bin/bash
# Delete the resources created by deploy_lambda_iam_cli.sh (NOT the CloudFormation
# stack): the Lambda event source mapping(s), the function, its execution role,
# the DynamoDB table, and the Secrets Manager secrets it created.
set -o pipefail
export AWS_PAGER=""

REGION="${AWS_REGION:-us-west-2}"
FUNCTION_NAME="${FUNCTION_NAME:-kafka-iam-consumer}"
DDB_TABLE_NAME="${DDB_TABLE_NAME:-KafkaIamAuth}"
ROLE_NAME="${ROLE_NAME:-${FUNCTION_NAME}-role}"
SECRET_NAMES=()

echo "Deleting event source mapping(s) for $FUNCTION_NAME..."
for uuid in $(aws lambda list-event-source-mappings --region "$REGION" --function-name "$FUNCTION_NAME" --query 'EventSourceMappings[].UUID' --output text 2>/dev/null); do
  [ -n "$uuid" ] && [ "$uuid" != "None" ] || continue
  aws lambda delete-event-source-mapping --region "$REGION" --uuid "$uuid" >/dev/null 2>&1 && echo "  deleting ESM $uuid"
done
# Wait for the mappings to disappear (deleting a function with a live ESM can fail).
for i in $(seq 1 30); do
  n=$(aws lambda list-event-source-mappings --region "$REGION" --function-name "$FUNCTION_NAME" --query 'length(EventSourceMappings)' --output text 2>/dev/null)
  { [ -z "$n" ] || [ "$n" = "0" ] || [ "$n" = "None" ]; } && break
  sleep 10
done

echo "Deleting function $FUNCTION_NAME..."
aws lambda delete-function --region "$REGION" --function-name "$FUNCTION_NAME" 2>/dev/null && echo "  deleted" || echo "  (not found)"

echo "Deleting DynamoDB table $DDB_TABLE_NAME..."
aws dynamodb delete-table --region "$REGION" --table-name "$DDB_TABLE_NAME" >/dev/null 2>&1 && echo "  deleted" || echo "  (not found)"

echo "Deleting execution role $ROLE_NAME..."
for p in $(aws iam list-role-policies --role-name "$ROLE_NAME" --query 'PolicyNames' --output text 2>/dev/null); do
  aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "$p" 2>/dev/null || true
done
for a in $(aws iam list-attached-role-policies --role-name "$ROLE_NAME" --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null); do
  aws iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn "$a" 2>/dev/null || true
done
aws iam delete-role --role-name "$ROLE_NAME" 2>/dev/null && echo "  deleted" || echo "  (not found)"

if [ "${#SECRET_NAMES[@]}" -gt 0 ]; then
  for s in "${SECRET_NAMES[@]}"; do
    echo "Deleting secret $s..."
    aws secretsmanager delete-secret --region "$REGION" --secret-id "$s" --force-delete-without-recovery >/dev/null 2>&1 && echo "  deleted" || echo "  (not found)"
  done
fi

echo "Done. The CloudFormation stack and the S3 Kafka/cert cache bucket are left intact."
