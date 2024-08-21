#!/bin/bash

start_time=$(date +%s)


for FIG_NUM in 03a 03b 03c 03d 04a 04b 04c 12 13 14 15a 15b 16 17 18a 18b 18c 18d 18e 18f_19b 19a 19c; do
  python3 fig_${FIG_NUM}.py
  sleep 1m
done


end_time=$(date +%s)
cost_time=$(($end_time-$start_time))
echo "Used time: $(($cost_time/60)) min $(($cost_time%60)) s"
