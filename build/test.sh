#!/bin/bash
for ((t=1;t<=1024;t*=2))
do
    for m in 1 2 3
    do
         ./double_change 384 384 ${t} set  &> 20-384-batch-test-${t}-${m}.log	
    done
done

