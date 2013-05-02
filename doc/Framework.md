1. Generate a list of image filenames as input
2. Mappers: input (key: LongWritable, value: Text as `${image-id} ${image-name}`) output (key: IntWritable, value: Text)
  1. emit key as `${image-id}` and value as `{image-name}`
3. Reducers: input (key: IntWritable, value: Text) output (key: Text, value: VectorWritable)
  1. Read image from HDFS with path in value
  2. Extract features for image
  3. emit key as an empty text and value as the feature vector
4. Kmeans clustering to build codebook
  1. input: `${hdfs-directory-feature}`
  2. output: `${hdfs-directory-codebook}`
5. LDA or Spatial LDA
  1. input: `${hdfs-directory-codebook}`
  2. output: `${hdfs-directory-results}/[lda|slda]`
