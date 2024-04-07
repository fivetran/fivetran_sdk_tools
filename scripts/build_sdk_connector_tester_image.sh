#!/bin/bash

set -e

VERSION="024.0322.001"

cd "$(git rev-parse --show-toplevel)"

bazel build //testers:run_sdk_connector_tester_deploy.jar

cp -f "$(git rev-parse --show-toplevel)/bazel-bin/testers/run_sdk_connector_tester_deploy.jar" .

docker buildx build --push -f Dockerfile.connector_tester --platform=linux/amd64,linux/arm64 --tag fivetrandocker/sdk-connector-tester:$VERSION  .

docker pull --platform=linux/amd64 fivetrandocker/sdk-connector-tester:$VERSION

# clean up
rm run_sdk_connector_tester_deploy.jar
