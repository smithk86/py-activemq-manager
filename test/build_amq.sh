#!/bin/sh

if [ -z $1 ]; then
    echo "usage: $0 [ACTIVEMQ_VERSION]"
    echo "example: $0 5.15.9"
    exit 1
fi

ACTIVEMQ_VERSION=$1

docker build \
    --build-arg ACTIVEMQ_VERSION=${ACTIVEMQ_VERSION} \
    --tag activemq_manager_testing:${ACTIVEMQ_VERSION} \
    activemq
