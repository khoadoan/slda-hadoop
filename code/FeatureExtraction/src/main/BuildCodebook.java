import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.colt.Arrays;

public class BuildCodebook extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(BuildCodebook.class);

  public void TransformVectorsToSequence(Configuration conf, String inputPath, String outputPath)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path inPath = new Path(inputPath);
    Path outPath = new Path(outputPath);

    SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(outPath),
        SequenceFile.Writer.keyClass(Text.class),
        SequenceFile.Writer.valueClass(VectorWritable.class));

    Text key = new Text();
    VectorWritable value = new VectorWritable();

    FSDataInputStream fdstream = fs.open(inPath);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fdstream));

    String line = "";
    int id = 0;
    while ((line = reader.readLine()) != null) {
      StringTokenizer token = new StringTokenizer(line);
      int numTokens = token.countTokens();
      NamedVector vec = new NamedVector();
      for (int i = 0; i < numTokens; ++i)
        vec.setQuick(i, Double.parseDouble(token.nextToken()));
      key.set(String.valueOf(++id));
      value.set(vec);
      writer.append(key, value);
    }
    writer.close();
  }

  @SuppressWarnings("deprecation")
  public void RenameClusterIds(Configuration conf, Path input, Path output) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    HadoopUtil.delete(conf, output);
    Path inputPathPattern;
    if (fs.getFileStatus(input).isDir())
      inputPathPattern = new Path(input, "*");
    else
      inputPathPattern = input;
    FileStatus[] inputFiles = fs.globStatus(inputPathPattern, PathFilters.logsCRCFilter());
    int clusterId = 0;
    IntWritable newKey = new IntWritable();
    ClusterWritable newValue = new ClusterWritable();
    for (FileStatus fileStatus : inputFiles) {
      if (fileStatus.isDir())
        continue;
      Path outFile = new Path(output, fileStatus.getPath().getName());
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outFile, IntWritable.class,
          ClusterWritable.class);
      for (Pair<Writable, ClusterWritable> record : new SequenceFileIterable<Writable, ClusterWritable>(
          fileStatus.getPath(), true, conf)) {
        ClusterWritable value = record.getSecond();
        Kluster cluster = (Kluster) value.getValue();
        Kluster newCluster = new Kluster(cluster.getCenter(), ++clusterId, cluster.getMeasure());
        
        newKey.set(clusterId);
        newValue.setValue(newCluster);
        writer.append(newKey, newValue);
      }
      writer.close();
    }
  }

  public void KMeansClustering(Configuration conf, String inputSequencePath,
      String outputSequencePath, int numClusters) throws IOException, InterruptedException,
      ClassNotFoundException {

    DistanceMeasure measure = new EuclideanDistanceMeasure();

    Path input = new Path(inputSequencePath);
    Path output = new Path(outputSequencePath);
    FileSystem.get(conf).delete(output, true);

    // Initial clustering
    LOG.info("Running random seed to get initial clusters");
    Path clusters = new Path(outputSequencePath, Cluster.INITIAL_CLUSTERS_DIR);
    clusters = RandomSeedGenerator.buildRandom(conf, input, clusters, numClusters, measure);

    // Kmeans clustering

    double convergenceDelta = 1e-6;
    int maxIterations = 1000;
    double clusterClassificationThreshold = 0.0;
    String delta = Double.toString(convergenceDelta);

    LOG.info("Running KMeans");
    LOG.info("Input: {} Clusters In: {} Out: {} Distance: {}", new Object[] { input, clusters,
        output, measure.getClass().getName() });
    LOG.info("convergence: {} max Iterations: {} num Reduce Tasks: {} Input Vectors: {}",
        new Object[] { convergenceDelta, maxIterations, VectorWritable.class.getName() });

    Path clustersOut = KMeansDriver.buildClusters(conf, input, clusters, output, measure,
        maxIterations, delta, false);

    Path clustersFinal = new Path(output, "clusters-final");

    LOG.info("Renaming cluster IDs");
    RenameClusterIds(conf, clustersOut, clustersFinal);

    LOG.info("Clustering data");
    KMeansDriver.clusterData(conf, input, clustersFinal, output, measure,
        clusterClassificationThreshold, false);

  }

  public void KMeansByMahout(Configuration conf, String inputPath, String outputPath, int K)
      throws Exception {

    DistanceMeasure measure = new EuclideanDistanceMeasure();

    // Read from text input data and transform it to Sequence File
    // TransformVectorsToSequence(conf, inputPath, sequencePath);
    Path input = new Path(inputPath);
    String sequencePath = "codebook/input-serial";
    Path sequence = new Path(sequencePath);
    FileSystem.get(conf).delete(sequence, true);

    LOG.info("Preparing Input");
    InputDriver.runJob(input, sequence, "org.apache.mahout.math.DenseVector");

    Path output = new Path(outputPath);
    FileSystem.get(conf).delete(output, true);

    // Initial clustering
    LOG.info("Running random seed to get initial clusters");
    Path clusters = new Path(output, Cluster.INITIAL_CLUSTERS_DIR);
    clusters = RandomSeedGenerator.buildRandom(conf, sequence, clusters, K, measure);

    // Kmeans clustering

    double convergenceDelta = 1e-3;
    int maxIterations = 100;
    LOG.info("Running KMeans");
    KMeansDriver.run(conf, sequence, clusters, output, measure, convergenceDelta, maxIterations,
        true, 0.0, false);

    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(new Path(output, "clusters-*-final"), new Path(
        output, "clusteredPoints"));
    // clusterDumper.printClusters(null);

    String textoutput = "HumanReadableClusters";
    FSDataOutputStream fsout = FileSystem.get(conf).create(new Path(output, textoutput));
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));
    Map<Integer, List<WeightedVectorWritable>> clusterMap = clusterDumper.getClusterIdToPoints();
    for (Map.Entry<Integer, List<WeightedVectorWritable>> entry : clusterMap.entrySet()) {
      writer.write(entry.toString() + "\n");
    }
    writer.close();
  }

  /**
   * Creates an instance of this tool.
   */
  public BuildCodebook() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_CLUSTERS = "K";
  private static final String FUNC = "func";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path")
        .create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path")
        .create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of clusters").create(NUM_CLUSTERS));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("tools")
        .create(FUNC));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int numClusters = cmdline.hasOption(NUM_CLUSTERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_CLUSTERS)) : 1024;
    String func = cmdline.hasOption(FUNC) ? cmdline.getOptionValue(FUNC) : "all";

    LOG.info("Tool: " + BuildCodebook.class.getSimpleName());

    Configuration conf = getConf();

    // Kmeans using mahout
    if (func.equals("all")) {
    KMeansClustering(conf, inputPath, outputPath, numClusters);
    } else if (func.equals("rename")){
      Path input = new Path(inputPath);
      Path output = new Path(outputPath);
      RenameClusterIds(conf, input, output);
    }

    long startTime = System.currentTimeMillis();
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildCodebook(), args);
  }
}
