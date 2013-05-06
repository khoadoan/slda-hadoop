#!/bin/bash
data=MSRCv1
K=200
etc/hadoop-cluster.sh MrDenseSift -input ang/$data/input.path -output ang/$data/feature -numReducers 16 -stepsize 8
etc/hadoop-cluster.sh BuildCodebook -input ang/$data/feature -output ang/$data/codebook -K $K
