#!/bin/bash
for ((t=64;t<=1024;t*=2))
do
    for m in 1 2 3
    do
         ./double_buf_get 384 384 ${t} get  &> 1-read-hash-nonuma-${t}-${m}.log	
    done
done

