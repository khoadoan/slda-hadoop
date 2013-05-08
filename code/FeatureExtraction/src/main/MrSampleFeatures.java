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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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

public class MrSampleFeatures extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(MrSampleFeatures.class);

  private static class MyMapper extends Mapper<Text, VectorWritable, DoubleWritable, VectorWritable> {

    @Override
    public void map(Text key, VectorWritable value, Context context) throws IOException,
        InterruptedException {

      if (Math.random() < context.getConfiguration().getFloat("ratio", (float)1.0)) {
        context.write(new DoubleWritable(Math.random()), value);
      }
    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<DoubleWritable, VectorWritable, Text, VectorWritable> {

    private static final Text KEY = new Text();
    @Override
    public void reduce(DoubleWritable key, Iterable<VectorWritable> values, Context context) throws IOException,
        InterruptedException {

      Iterator<VectorWritable> iter = values.iterator();
      
      while (iter.hasNext()) {
        context.write(KEY, iter.next());
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public MrSampleFeatures() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";
  private static final String RATIO = "ratio";

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
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("sample ratio")
        .create(RATIO));

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
    float ratio = cmdline.hasOption(RATIO) ? Float.parseFloat(cmdline
        .getOptionValue(RATIO)) : 1;

    LOG.info("Tool: " + MrSampleFeatures.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reducerTasks);
    LOG.info(" - ratio: " + ratio);

    Configuration conf = getConf();
    conf.setFloat("ratio", ratio);
    Job job = Job.getInstance(conf);
    job.setJobName(MrSampleFeatures.class.getSimpleName());
    job.setJarByClass(MrSampleFeatures.class);

    job.setNumReduceTasks(reducerTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(DoubleWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);

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
    ToolRunner.run(new MrSampleFeatures(), args);
  }
}