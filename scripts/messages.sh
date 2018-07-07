#!/usr/bin/env bash

# set -euxo pipefail

topic="$1"
tmp=/tmp/data.json

fetch () {
    nextToken="$1"

    if [ -n "$nextToken" ]
    then
        curl -fs "localhost:8080/messages/$topic?nextToken=$nextToken" > $tmp
    else
        curl -fs "localhost:8080/messages/$topic" > $tmp
    fi

    # cat $tmp | jq '.messages[]' -c

    echo "read $(cat $tmp | jq '.messages[]' -c | wc -l) messages, last message $(cat $tmp | jq '.messages[]' -c | tail -n 1) with token $nextToken"

    sleep 1
    fetch $(cat $tmp | jq .nextToken -r)
}

fetch ""
