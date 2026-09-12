#!/bin/bash
# Negative test: an IAM identity that cannot authenticate to MSK.
# Uses a role ARN that cannot be assumed / has no MSK permissions, so the
# AWS_MSK_IAM handshake fails.
source /home/ec2-user/kafka_oauth.env

echo "=================================================================="
echo " Negative test: unauthorized / invalid IAM identity"
echo "=================================================================="
BADPROPS=/tmp/bad-iam.properties
cat > "$BADPROPS" <<EOF
bootstrap.servers=$BOOTSTRAP_SERVERS
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::000000000000:role/does-not-exist" awsStsRegion="$AWS_REGION";
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF
"$KAFKA_HOME"/bin/kafka-topics.sh --list \
  --bootstrap-server "$BOOTSTRAP_SERVERS" --command-config "$BADPROPS" 2>&1 | head -15
echo ">> expected: SASL authentication failure (role cannot be assumed / not authorized)"
