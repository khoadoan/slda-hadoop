#!/bin/bash
data=MSRCv1
K=1024
echo "\$0 : " $0
echo "\$1 : " $1
echo "\$2 : " $2

if [ $1 = "feature" ]; then
etc/hadoop-cluster.sh MrDenseSift -input ang/$data/input.path -output ang/$data/feature -numReducers 64 -stepsize 8
fi

if [ $1 = "codebook" ]; then
etc/hadoop-cluster.sh BuildCodebook -input ang/$data/feature/ -sample ang/$data/sample-feature -output ang/$data/codebook -K $K
fi

if [ $1 = "slda-input" ]; then
etc/hadoop-cluster.sh Convert -input ang/$data/codebook/clusteredPoints -output ang/$data/slda-input -numReducers 24 
fi

if [ $1 = "interpret" ]; then
etc/hadoop-cluster.sh Interpret -input ang/$data/slda-input/part-r-00000 -output ang/$data/slda-input-0.readable -func general
hadoop fs -get ang/$data/slda-input-0.readable $data-slda-input.readable
fi

if [ $1 = "slda-convert" ]; then
	etc/hadoop-cluster.sh ConvertSLDA -input khoa/output/slda-annotation -output ang/$data/slda-output -numReducers 1
fi

if [ $1 = "join" ]; then
etc/hadoop-cluster.sh JoinCodebookTopic -input ang/$data/slda-input -topic ang/$data/slda-output/part-r-00000 -output ang/$data/topic -numReducers 1 
fi
