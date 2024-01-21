#!/bin/bash

docker rm -vf $(docker ps -a -q --filter ancestor=lb-sim/worker:latest)

docker ps
