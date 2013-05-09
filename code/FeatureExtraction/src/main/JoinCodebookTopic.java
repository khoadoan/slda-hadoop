import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.io.MapFile;
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
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.map.HashMapWritable;
import edu.umd.cloud9.io.pair.PairOfInts;

public class JoinCodebookTopic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(JoinCodebookTopic.class);

  private static class MyMapper extends Mapper<IntWritable, HashMapWritable<PairOfInts, IntWritable>, IntWritable, HashMapWritable<PairOfInts, IntWritable>> {

    @Override
    public void map(IntWritable key, HashMapWritable<PairOfInts, IntWritable> value, Context context) throws IOException,
        InterruptedException {
      context.write(key, value);
    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<IntWritable, HashMapWritable<PairOfInts, IntWritable>, IntWritable, HashMapWritable<PairOfInts, IntWritable>> {

    private static final IntWritable VALUE = new IntWritable();
    private static MapFile.Reader reader;
    private static HMapSIW val = new HMapSIW();
    
    @SuppressWarnings("deprecation")
    public void setup(Context context) throws IOException {
      reader = new MapFile.Reader(FileSystem.get(context.getConfiguration()),
          context.getConfiguration().get("topic"),
          context.getConfiguration());
    }
    
    @Override
    public void reduce(IntWritable key, Iterable<HashMapWritable<PairOfInts, IntWritable>> values, Context context) throws IOException,
        InterruptedException {

      Iterator<HashMapWritable<PairOfInts, IntWritable>> iter = values.iterator();
      
      reader.get(new Text(key.toString()), val);
      
      if (iter.hasNext()) {
        HashMapWritable<PairOfInts, IntWritable> maps = iter.next();
        for (Map.Entry<PairOfInts, IntWritable> item : maps.entrySet()) {
          VALUE.set(val.get(item.getValue().toString()));
          item.setValue(VALUE);
        }
        context.write(key, maps);
      }
      
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public JoinCodebookTopic() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";
  private static final String TOPIC = "topic";

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
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("topic path")
        .create(TOPIC));

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
    String topicPath = cmdline.getOptionValue(TOPIC);
    int reducerTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + JoinCodebookTopic.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reducerTasks);
    LOG.info(" - topic path: " + topicPath);

    Configuration conf = getConf();
    conf.set("topic", topicPath);
    Job job = Job.getInstance(conf);
    job.setJobName(JoinCodebookTopic.class.getSimpleName());
    job.setJarByClass(JoinCodebookTopic.class);

    job.setNumReduceTasks(reducerTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(HashMapWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(HashMapWritable.class);

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
    ToolRunner.run(new JoinCodebookTopic(), args);
  }
}