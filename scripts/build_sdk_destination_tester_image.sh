#!/bin/bash

set -e

VERSION="024.0222.002"

# -- Don't forget the following --
# gcloud auth login
# docker login

cd "$(git rev-parse --show-toplevel)"

# Copy the latest proto files
[ -f "common.proto" ] && [ "common.proto" -ot "../common.proto" ] && rm "common.proto"
[ -f "connector_sdk.proto" ] && [ "connector_sdk.proto" -ot "../connector_sdk.proto" ] && rm "connector_sdk.proto"
[ -f "destination_sdk.proto" ] && [ "destination_sdk.proto" -ot "../destination_sdk.proto" ] && rm "destination_sdk.proto"
cp -p ../fivetran_sdk/*.proto .

bazel build //testers:run_sdk_destination_tester_deploy.jar

cp -f "$(git rev-parse --show-toplevel)/bazel-bin/testers/run_sdk_destination_tester_deploy.jar" .

docker buildx build --push -f Dockerfile.destination_tester --platform=linux/amd64,linux/arm64 --tag fivetrandocker/sdk-destination-tester:$VERSION  .

docker pull --platform=linux/amd64 fivetrandocker/sdk-destination-tester:$VERSION

# clean up
rm run_sdk_destination_tester_deploy.jar
