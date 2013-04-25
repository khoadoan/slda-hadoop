##### Codebook Construction via Mahout Kmeans Clustering 
* Source code: `code/MahoutKmeans/src/main/BuildCodebook.java`
* Command line: `hadoop -input ${input-dir} -output ${output-dir} -K ${number-of-clusters}`

###### Input format
Two types of input format can be specified:

1. SequenceFile
 1. Key: `Text/LongWritable`
 2. Value: `org.apache.mahout.math.RandomAccessSparseVector`
2. ASCII File
 1. Each row contains a double-valued vector separated by single space

###### Output format
The final clustering result is located at `${output-dir}/clusters-*-final`, where * is the number of iterations.
The file is a SequenceFile, each entry of which contains points belong to the same cluster:

1. Key: Text/LongWritable
2. Value: `List<WeightedVectorWritable>`

There is also a human readable clustering output file which are transformed using ClusterDumper, located at `${output-dir}/ClustersHumanReadable`
