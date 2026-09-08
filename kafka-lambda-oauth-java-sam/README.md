# kafka-lambda-oauth-java-sam
# Java AWS Lambda consumer for a self-managed Apache Kafka cluster with OAuth (Amazon Cognito) authentication, using AWS SAM

This pattern shows a Lambda function that consumes messages from a **self-managed Apache Kafka** cluster running on Amazon EC2, where the cluster is configured to use **SASL/OAUTHBEARER** authentication with an **Amazon Cognito User Pool** as the OAuth 2.0 identity provider.

It is the self-managed Kafka + OAuth counterpart of the [`msk-lambda-iam-java-sam`](../msk-lambda-iam-java-sam) pattern (which uses Amazon MSK with IAM auth).

This project contains source code and supporting files for a serverless application that you can deploy with the SAM CLI. It includes the following files and folders:

- `kafka_event_consumer_function/src/main/java` - Code for the application's Lambda function.
- `events` - Invocation events that you can use to invoke the function.
- `kafka_event_consumer_function/src/test/java` - Unit tests for the application code.
- `template_original.yaml` - A SAM template that defines the Lambda function and its self-managed Apache Kafka event source. Placeholder values are substituted into a copy named `template.yaml` on the client EC2 instance.
- `KafkaBrokersCognitoClientEC2.yaml` - A CloudFormation template that deploys the self-managed Kafka cluster (3 broker EC2 instances), an Amazon Cognito User Pool, and a client EC2 instance with all pre-requisites installed, so you can build, deploy and test the Lambda function.

Important: this application uses various AWS services and there are costs associated with these services after the Free Tier usage - please see the [AWS Pricing page](https://aws.amazon.com/pricing/) for details. You are responsible for any AWS costs incurred. No warranty is implied in this example.

## Architecture

```
                         Amazon Cognito User Pool  (OAuth 2.0 IdP / JWKS)
                                     ^        ^
                    (validate JWT)   |        |  (USER_PASSWORD_AUTH -> access token)
                                     |        |
  +----------------------- VPC (10.0.0.0/16) -+--------------------------------+
  |                                                                            |
  |  Private subnets (3 AZs)                       Public subnet               |
  |  +------------------+  +----------------+  +----------------+   +--------+  |
  |  | KafkaBroker1     |  | KafkaBroker2   |  | KafkaBroker3   |   | Client |  |
  |  | 10.0.1.10:9092   |  | 10.0.2.10:9092 |  | 10.0.3.10:9092 |   |  EC2   |  |
  |  | KRaft            |  | KRaft          |  | KRaft          |   | (SAM)  |  |
  |  | SASL/OAUTHBEARER |  |                |  |                |   +--------+  |
  |  +------------------+  +----------------+  +----------------+               |
  |            ^                                                                |
  |            | (ESM ENIs in VPC subnets)                                      |
  +------------|----------------------------------------------------------------+
               |
        AWS Lambda (self-managed Kafka event source mapping)
```

Key design points:

* **Self-managed Kafka**: 3 brokers on 3 EC2 instances, one per Availability Zone, running Apache Kafka in **KRaft** mode (no ZooKeeper). Each broker has a fixed private IP (`10.0.1.10`, `10.0.2.10`, `10.0.3.10`) so the KRaft quorum and bootstrap servers are known ahead of time.
* **OAuth authentication**: The broker client listener uses `SASL_PLAINTEXT` with the `OAUTHBEARER` mechanism, implemented with the [Strimzi kafka-oauth](https://github.com/strimzi/strimzi-kafka-oauth) libraries. Brokers validate incoming JWT access tokens against the Cognito User Pool's JWKS endpoint (`https://cognito-idp.<region>.amazonaws.com/<userPoolId>/.well-known/jwks.json`).
* **Amazon Cognito**: The CloudFormation template creates a User Pool, an app client (no secret, `USER_PASSWORD_AUTH` enabled) and an admin user. The admin **username and password are CloudFormation parameters**. The client EC2 instance exchanges them for a Cognito access token via `cognito-idp initiate-auth` and passes the token to Kafka using Strimzi's `oauth.access.token` client option.

## Requirements

* [Create an AWS account](https://portal.aws.amazon.com/gp/aws/developer/registration/index.html) if you do not already have one and log in. The IAM user that you use must have sufficient permissions to make necessary AWS service calls and manage AWS resources.

## Run the CloudFormation template to create the Kafka cluster, Cognito User Pool and client EC2 machine

Deploy the file `KafkaBrokersCognitoClientEC2.yaml` from the AWS CloudFormation console (or via the CLI). You must supply:

* **CognitoAdminUsername** - Username for the Cognito admin user (default `kafka-admin`).
* **CognitoAdminPassword** - Password for the Cognito admin user. Must satisfy the pool password policy: minimum 8 characters, with at least one upper case letter, one lower case letter and one number. **Do not use a comma in the password** (it is passed through `--auth-parameters` on the client instance).

You can keep the defaults for the remaining parameters or adjust them (Java version, Kafka download URL, Strimzi/Nimbus library versions, Kafka topic name, etc.). Wait for the stack to reach `CREATE_COMPLETE`.

This template creates:
* A VPC with 1 public and 3 private subnets, an Internet Gateway and a NAT Gateway.
* 3 Kafka broker EC2 instances (KRaft mode, OAUTHBEARER auth) in the private subnets.
* An Amazon Cognito User Pool, app client and admin user.
* A client EC2 instance in the public subnet with Java, Maven, Docker, the AWS CLI, the AWS SAM CLI, Kafka CLI tools and the Strimzi OAuth client libraries installed.
* An EC2 Instance Connect Endpoint for SSH access.

> **Note on library versions:** The brokers download the Strimzi `kafka-oauth-*` libraries and `nimbus-jose-jwt` from Maven Central at boot (versions are CloudFormation parameters). If you change the Apache Kafka version, you may need to pick a compatible Strimzi OAuth version.

* [Connect to the client EC2 machine] - Once the stack is created, go to the EC2 console, select the `KafkaClientInstance` and use "Connect using EC2 Instance Connect Endpoint". You may need to wait a few minutes after `CREATE_COMPLETE`, because the UserData scripts continue running after the stack shows created (installing SAM, downloading Kafka, creating the topic, etc.).

* [Check the broker setup] - On each broker instance the setup log is at `/var/log/kafka-broker-setup.log` and Kafka runs as a systemd service (`sudo systemctl status kafka`).

* [Check if the Kafka topic was created] - On the client instance (in `/home/ec2-user`) run `cat kafka_topic_creator_output.txt`. You should see the topic listed. If the file is missing, empty or shows an error (for example because the brokers were not ready yet), re-run `./kafka_topic_creator.sh`.

## How authentication works

The client instance stores the Cognito details in `/home/ec2-user/.bash_profile` and provides a helper script `refresh_token.sh` that:

1. Calls `aws cognito-idp initiate-auth --auth-flow USER_PASSWORD_AUTH ...` with the admin username/password to obtain a JWT **access token**.
2. Writes `/home/ec2-user/kafka/config/client.properties` with:

   ```properties
   security.protocol=SASL_PLAINTEXT
   sasl.mechanism=OAUTHBEARER
   sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler
   sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token="<JWT>" ;
   ```

Cognito access tokens are valid for 1 hour, so `kafka_topic_creator.sh` and `kafka_message_sender.sh` both call `refresh_token.sh` first to write a fresh token before talking to the cluster.

## Pre-requisites to deploy the sample Lambda function

The client EC2 machine already has everything you need (Java, Maven, AWS SAM CLI, Docker) and the repository has already been cloned to `/home/ec2-user/serverless-patterns`.

Change directory to the pattern directory:

```bash
cd /home/ec2-user/serverless-patterns/kafka-lambda-oauth-java-sam
```

The CloudFormation UserData already created `template.yaml` from `template_original.yaml`, substituting the broker bootstrap servers, the VPC subnet IDs, the security group ID, the topic name and the Java version.

## Use the SAM CLI to build and test locally

Build your application with the `sam build` command.

```bash
sam build
```

Test the function locally with the sample event:

```bash
sam local invoke --event events/event.json
```

## Deploy the sample application

> **Important — OAuth event source support:** At the time of publishing, the AWS Lambda **self-managed Apache Kafka event source mapping does not yet support SASL/OAUTHBEARER authentication**. Support is expected soon. The `SelfManagedKafka` event in `template_original.yaml` is provided as scaffolding: it includes the bootstrap servers and the VPC subnet/security-group `SourceAccessConfigurations`, and a clearly-marked placeholder where the authentication `SourceAccessConfiguration` must be added once AWS releases OAUTHBEARER support. Until then, `sam deploy` will not create a working event source mapping, because an authentication configuration is required.

To deploy your application, run the following in your shell from the pattern directory:

```bash
sam deploy --capabilities CAPABILITY_IAM --no-confirm-changeset --no-disable-rollback --region $AWS_REGION --stack-name kafka-lambda-oauth-java-sam --guided
```

The `sam deploy --guided` command walks you through a series of prompts:

* **Stack Name**: The name of the stack to deploy to CloudFormation.
* **AWS Region**: The AWS region you want to deploy your app to.
* **Parameter KafkaTopic**: The Kafka topic the Lambda function will consume from (already substituted as the default).
* **Confirm changes before deploy**, **Allow SAM CLI IAM role creation**, **Disable rollback**, **Save arguments to configuration file**: accept the defaults.

## Test the sample application

Once the Lambda function is deployed (and once OAuth support for the event source is available and configured), send some Kafka messages on the topic.

On the client EC2 machine:

```bash
cd /home/ec2-user
sh kafka_message_sender.sh
>My first message
>My second message
>My third message
...
>Ctrl-C
```

Either send at least 10 messages or wait 300 seconds (see `BatchSize: 10` and `MaximumBatchingWindowInSeconds: 300` in `template.yaml`).

Then check CloudWatch Logs for the deployed Lambda function's log group. The function parses each Kafka message and logs its fields (topic, partition, offset, timestamp, key/value — base64-decoded — and headers).

A single Lambda invocation receives a batch of messages as a map, keyed by `topic-partition` (one batch can contain messages from multiple partitions). Each key holds a list of messages. The key and value of each message are base64-encoded and are decoded by the handler.

## Cleanup

1. Delete the Lambda application:

   ```bash
   cd /home/ec2-user/serverless-patterns/kafka-lambda-oauth-java-sam
   sam delete
   ```

   Confirm `y` for both prompts.

2. Delete the CloudFormation stack that created the Kafka brokers, Cognito User Pool and client EC2 instance from the CloudFormation console (select the stack and choose "Delete"). If deletion fails, retry with a Force Delete — ENIs created by the deployed Lambda function in the VPC can occasionally delay VPC deletion even after the function is removed.
