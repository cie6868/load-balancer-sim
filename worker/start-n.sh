#!/bin/bash

for ((i = 1; i <= $2; i++))
do
  docker run -d -e lb_type=$1 -e power=100 -e worker_id=$((6800 + $i)) --name="$1_worker_$((6800 + $i))" -p $((6800 + $i)):$((6800 + $i)) lb-sim/worker
done

docker ps
