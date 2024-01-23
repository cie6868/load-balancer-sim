#!/bin/bash

for ((i = 1; i <= $(($2 / 2)); i++))
do
  docker run -d -e lb_type=$1 -e power=100 -e worker_id=$((6800 + $i)) -e lb_port=6799 --name="$1_worker_100_$((6800 + $i))" --net host lb-sim/worker
done

for ((i = $((($2 / 2) + 1)); i <= $2; i++))
do
  docker run -d -e lb_type=$1 -e power=120 -e worker_id=$((6800 + $i)) -e lb_port=6799 --name="$1_worker_120_$((6800 + $i))" --net host lb-sim/worker
done

docker ps
