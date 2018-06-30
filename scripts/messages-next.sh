#!/bin/bash
curl -v "localhost:8080/messages/topic-0?nextToken=$1"
