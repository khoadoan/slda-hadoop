import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import mpicbg.imagefeatures.Feature;

import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayOfFloatsWritable;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.io.triple.TripleOfInts;

public class MrDenseSift extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(MrDenseSift.class);

  private static class MyMapper extends Mapper<LongWritable, Text, TripleOfInts, VectorWritable> {

    // Reuse objects.
    private final static PairOfFloats CENTER = new PairOfFloats();
    private final static Text FILE_PATH = new Text();
    private final static TripleOfInts KEY = new TripleOfInts();
    private final static VectorWritable FEATURE_VECTOR = new VectorWritable();
    private static RandomAccessSparseVector VECTOR = new RandomAccessSparseVector(128);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {

      //
      String[] str = value.toString().split("\t");
      String filePath = str[0];
      int id = Integer.parseInt(str[1]);

      FSDataInputStream stream = FileSystem.get(context.getConfiguration())
          .open(new Path(filePath));

      List<Feature> extractedSiftFeatures = new ExtractDenseSiftFromImage(stream, 16)
          .getExtractedFeatures();

      for (Feature f : extractedSiftFeatures) {
        // float[] centerLocation = f.location;
        // CENTER.set(centerLocation[0], centerLocation[1]);
        // FILE_PATH.set(filePath);
        KEY.set(id, (int) f.location[0], (int) f.location[1]);
        if (f.descriptor.length != VECTOR.size())
          VECTOR = new RandomAccessSparseVector(f.descriptor.length);
        for (int i = 0; i < f.descriptor.length; ++i)
          VECTOR.set(i, (double) f.descriptor[i]);
        FEATURE_VECTOR.set(VECTOR);

        context.write(KEY, FEATURE_VECTOR);
      }

      stream.close();
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public MrDenseSift() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_MAPPERS = "numMappers";

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
        .withDescription("number of mappers").create(NUM_MAPPERS));

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
    int mapTasks = cmdline.hasOption(NUM_MAPPERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_MAPPERS)) : 1;

    LOG.info("Tool: " + MrDenseSift.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of mappers: " + mapTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(MrDenseSift.class.getSimpleName());
    job.setJarByClass(MrDenseSift.class);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(TripleOfInts.class);
    job.setOutputValueClass(VectorWritable.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new MrDenseSift(), args);
  }
}