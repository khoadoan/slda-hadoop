1. Generate a list of image filenames as input
2. MapReduce: for each map (key: LongWritable, value: Text as `${filename-i}`)
2.1. copy `${filename-i}` from HDFS to local address
  2.2. extract features and store at local address `${filename-feature}`
  2.3. copy `${filename-feature}` from local to HDFS `${hdfs-directory-feature}`
  2.4. remove local cache files
3. Kmeans clustering to build codebook
  1. input: `${hdfs-directory-feature}`
  2. output: `${hdfs-directory-codebook}`
4. LDA or Spatial LDA
  1. input: `${hdfs-directory-codebook}`
  2. output: `${hdfs-directory-results}/[lda|slda]`
