#!/bin/bash

echo $lb_type
echo $lb_hostname
echo $lb_port
python ${lb_type}.py ${worker_id} ${power} ${lb_hostname} ${lb_port}
