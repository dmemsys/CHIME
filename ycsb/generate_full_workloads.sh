#!/bin/bash

start_time=$(date +%s)

for WORKLOAD_TYPE in la a b c d e 40M 60M 80M 100M 120M; do
  python3 gen_workload.py workload${WORKLOAD_TYPE} randint full
done

end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Used time: $(($cost_time/60)) min $(($cost_time%60)) s"
