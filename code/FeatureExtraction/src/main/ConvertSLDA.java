import java.io.IOException;
import java.util.Iterator;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

import cern.colt.Arrays;
import edu.umd.cloud9.io.map.HMapIIW;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.map.HashMapWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.triple.TripleOfInts;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapKI;

public class ConvertSLDA extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ConvertSLDA.class);

  private static class ConvertSLDAMapper extends Mapper<Text, HMapSIW, IntWritable, HMapIIW> {

    private final static IntWritable KEY = new IntWritable();

    @Override
    public void map(Text key, HMapSIW value, Context context) throws IOException,
        InterruptedException {

      KEY.set(Integer.parseInt(key.toString()));
      
      HMapIIW val = new HMapIIW();
      for (MapKI.Entry<String> item : value.entrySet())  {
        int k = Integer.parseInt(item.getKey());
        val.put(k, item.getValue());
      }
      
      context.write(KEY, val);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public ConvertSLDA() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";
  private static final String FUNC = "code2lda";

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
    options.addOption(OptionBuilder.withArgName("arg").hasArg().withDescription("type")
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
    int reducerTasks = 0;
    String func = cmdline.hasOption(FUNC) ? cmdline.getOptionValue(FUNC) : "";

    LOG.info("Tool: " + ConvertSLDA.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reducerTasks);
    LOG.info(" - type: " + func);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(ConvertSLDA.class.getSimpleName());
    job.setJarByClass(ConvertSLDA.class);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(HMapIIW.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(HMapIIW.class);

    job.setMapperClass(ConvertSLDAMapper.class);

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
    ToolRunner.run(new ConvertSLDA(), args);
  }
}
