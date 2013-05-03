import java.io.IOException;
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
import org.apache.hadoop.io.IntWritable;
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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import mpicbg.imagefeatures.Feature;

import cern.colt.Arrays;

public class MrDenseSift extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(MrDenseSift.class);

  private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private final static IntWritable KEY = new IntWritable();
    private final static Text VALUE = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {

      String[] str = value.toString().split("\t");

      KEY.set(Integer.parseInt(str[0]));
      VALUE.set(str[1]);
      context.write(KEY, VALUE);

    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<IntWritable, Text, Text, VectorWritable> {

    // Reuse objects.
    private FileSystem fs;
    private static int stepsize = 8;
    private final static VectorWritable FEATURE_VECTOR = new VectorWritable();
    private static DenseVector VECTOR = new DenseVector(128);
    private final static Text EMPTY = new Text();

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      stepsize = conf.getInt("stepsize", 8);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {

      int id = key.get();

      // there is only one value for each group
      Iterator<Text> iter = values.iterator();

      try {
        // take the value which is the file path and process the image
        if (iter.hasNext()) {

          String filePath = iter.next().toString();

          FSDataInputStream stream = fs.open(new Path(filePath));

          List<Feature> extractedSiftFeatures = new ExtractDenseSiftFromImage(stream, stepsize)
              .getExtractedFeatures();

          for (Feature f : extractedSiftFeatures) {
            if (f.descriptor.length != VECTOR.size())
              VECTOR = new DenseVector(f.descriptor.length);
            for (int i = 0; i < f.descriptor.length; ++i)
              VECTOR.set(i, (double) f.descriptor[i]);
            NamedVector NAMED_VECTOR = new NamedVector(VECTOR, String.valueOf(id) + " "
                + String.valueOf(f.location[0]) + " " + String.valueOf(f.location[1]));
            FEATURE_VECTOR.set(NAMED_VECTOR);

            context.write(EMPTY, FEATURE_VECTOR);
          }

          stream.close();
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public MrDenseSift() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";
  private static final String STEP_SIZE = "stepsize";

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
        .withDescription("number of reducers").create(NUM_REDUCERS));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("step size")
        .create(STEP_SIZE));

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
    int reducerTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;
    int stepsize = cmdline.hasOption(STEP_SIZE) ? Integer.parseInt(cmdline
        .getOptionValue(STEP_SIZE)) : 8;

    LOG.info("Tool: " + MrDenseSift.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reducerTasks);
    LOG.info(" - step size: " + stepsize);

    Configuration conf = getConf();
    conf.setInt("stepsize", stepsize);
    Job job = Job.getInstance(conf);
    job.setJobName(MrDenseSift.class.getSimpleName());
    job.setJarByClass(MrDenseSift.class);

    job.setNumReduceTasks(reducerTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(VectorWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

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