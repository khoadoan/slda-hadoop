#!/bin/bash

#etc/hadoop-cluster.sh MrDenseSift -input ang/MSRCv1.input -output ang/feature/MSRCv1/raw -numReducers 15 -stepsize 8
etc/hadoop-cluster.sh BuildCodebook -input ang/feature/MSRCv1/raw -output ang/feature/MSRCv1/codebook -K 200
