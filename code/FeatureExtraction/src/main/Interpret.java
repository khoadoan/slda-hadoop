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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

import cern.colt.Arrays;

public class Interpret extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(Interpret.class);

  public void InfoFeatures(Configuration conf, String inputPath, String outputPath)
      throws Exception {

    Path input = new Path(inputPath);

    Path output = new Path(outputPath);
    FileSystem.get(conf).delete(output, true);

    @SuppressWarnings("deprecation")
    SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), input, conf);

    int numPts = 0;
    FSDataOutputStream fsout = FileSystem.get(conf).create(output);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));
    
    IntWritable key = new IntWritable();
    VectorWritable val = new VectorWritable();
    while (reader.next(key, val)) {
      writer.write(key + "\t" + val.toString() + "\n");
      ++ numPts;
    }

    writer.write("Total number of points: " + String.valueOf(numPts) + "\n");
    writer.close();
  }
  
  public void InfoPointToCluster(Configuration conf, String inputPath, String outputPath)
      throws Exception {

    Path input = new Path(inputPath);

    Path output = new Path(outputPath);
    FileSystem.get(conf).delete(output, true);

    @SuppressWarnings("deprecation")
    SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(input, "clusteredPoints/part-m-00000"), conf);

    int numPts = 0;
    FSDataOutputStream fsout = FileSystem.get(conf).create(output);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));
    
    IntWritable key = new IntWritable();
    WeightedVectorWritable val = new WeightedVectorWritable();
    while (reader.next(key, val)) {
      writer.write(key + "\t" + ((NamedVector)val.getVector()).getName() + "\n");
      ++ numPts;
    }

    writer.write("Total number of points: " + String.valueOf(numPts) + "\n");
    writer.close();
  }
  
  public void InfoTextClusterWritable(Configuration conf, String inputPath, String outputPath)
      throws Exception {

    Path input = new Path(inputPath);

    Path output = new Path(outputPath);
    FileSystem.get(conf).delete(output, true);

    @SuppressWarnings("deprecation")
    SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), input, conf);

    int numPts = 0;
    FSDataOutputStream fsout = FileSystem.get(conf).create(output);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));
    
    Text key = new Text();
    ClusterWritable val = new ClusterWritable();
    while (reader.next(key, val)) {
      writer.write(key.toString() + "\t" + val.toString() + "\n");
      ++ numPts;
    }

    writer.write("Total number of Clusters: " + String.valueOf(numPts) + "\n");
    writer.close();
  }
  public void InfoClusters(Configuration conf, String inputPath, String outputPath)
      throws Exception {

    Path input = new Path(inputPath);

    Path output = new Path(outputPath);
    FileSystem.get(conf).delete(output, true);

    // run ClusterDumper
    ClusterDumper clusterDumper = new ClusterDumper(new Path(input, "clusters-*-final"), new Path(
        input, "clusteredPoints"));
    // clusterDumper.printClusters(null);

    int numPts = 0;
    FSDataOutputStream fsout = FileSystem.get(conf).create(output);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));
    Map<Integer, List<WeightedVectorWritable>> clusterMap = clusterDumper.getClusterIdToPoints();
    for (Map.Entry<Integer, List<WeightedVectorWritable>> entry : clusterMap.entrySet()) {
      writer.write("Cluster-ID: " + entry.getKey() + ", Vector-ID:");
      List<WeightedVectorWritable> list = entry.getValue();
      for (WeightedVectorWritable a : list) {
        NamedVector vec = (NamedVector) a.getVector();
        writer.write(" " + vec.getName());
        ++ numPts;
      }
      writer.write("\n");
    }
    writer.write("Total number of points: " + String.valueOf(numPts) + "\n");
    writer.close();
  }

  /**
   * Creates an instance of this tool.
   */
  public Interpret() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
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
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("function")
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
    String func = cmdline.getOptionValue(FUNC);

    LOG.info("Tool: " + Interpret.class.getSimpleName());

    Configuration conf = getConf();

    // Kmeans using mahout
    if (func.equals("clusters"))
      InfoClusters(conf, inputPath, outputPath);
    else if (func.equals("points"))
      InfoPointToCluster(conf, inputPath, outputPath);
    else if (func.equals("features"))
      InfoFeatures(conf, inputPath, outputPath);
    else if (func.equals("TextCluster"))
      InfoTextClusterWritable(conf, inputPath, outputPath);

    long startTime = System.currentTimeMillis();
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Interpret(), args);
  }
}
