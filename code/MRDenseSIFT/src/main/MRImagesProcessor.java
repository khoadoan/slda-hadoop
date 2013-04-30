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

import mpicbg.imagefeatures.Feature;

import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayOfFloatsWritable;
import edu.umd.cloud9.io.pair.PairOfFloats;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class MRImagesProcessor extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(MRImagesProcessor.class);
  
  private static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      // just passing the key-value pair
      context.write(key, value);
      
    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<LongWritable, Text, PairOfWritables<Text,PairOfFloats>, ArrayOfFloatsWritable> {

    // Reuse objects.
    private final static PairOfFloats CENTER = new PairOfFloats();
    private final static Text FILE_PATH = new Text();
    private final static PairOfWritables<Text,PairOfFloats> KEY = new PairOfWritables<Text,PairOfFloats>();
    private final static ArrayOfFloatsWritable FEATURE_VECTOR = new ArrayOfFloatsWritable();
    private FileSystem fs;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      fs = FileSystem.get(conf);
    }
    
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        
      // there is only one value for each group
      Iterator<Text> iter = values.iterator();
      
      try {
        // take the value which is the file path and process the image
        if (iter.hasNext()) {
          
          String filePath = iter.next().toString();
          
          FSDataInputStream stream = fs.open(new Path(filePath));
          
          List<Feature> extractedSiftFeatures = 
              new ExtractDenseSiftFromImage(stream, 16)
              .getExtractedFeatures();
          
          for (Feature f : extractedSiftFeatures) {
            float[] centerLocation = f.location;
            CENTER.set(centerLocation[0], centerLocation[1]);
            FILE_PATH.set(filePath);
            KEY.set(FILE_PATH, CENTER);
            FEATURE_VECTOR.setArray(f.descriptor);
            
            context.write(KEY, FEATURE_VECTOR);
          }
          
          stream.close();
        }
      } catch(Exception e) {System.out.println(e.getMessage());}
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public MRImagesProcessor() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

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
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + MRImagesProcessor.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(MRImagesProcessor.class.getSimpleName());
    job.setJarByClass(MRImagesProcessor.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(PairOfWritables.class);
    job.setOutputValueClass(ArrayOfFloatsWritable.class);

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
    ToolRunner.run(new MRImagesProcessor(), args);
  }
}