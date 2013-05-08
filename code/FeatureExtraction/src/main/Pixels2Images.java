
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import cern.colt.Arrays;
import edu.umd.cloud9.io.triple.TripleOfInts;

public class Pixels2Images extends Configured implements Tool {
    
    private static class Pixels2ImMapper extends 
        Mapper<TripleOfInts, IntWritable, IntWritable, TripleOfInts> {
        
    	private final static IntWritable KEY = new IntWritable();
    	private final static TripleOfInts VALUE = new TripleOfInts();
    	
        @Override
        public void map(TripleOfInts key, IntWritable value, Context context) throws IOException,
            InterruptedException {
            
        	KEY.set(key.getLeftElement());
        	VALUE.set(key.getMiddleElement(), key.getRightElement(), value.get());
        	
            context.write(KEY, VALUE);

        }
        
    }
    
    private static class Pixels2ImReducer extends Reducer
        <IntWritable, TripleOfInts, IntWritable, BytesWritable> {
    
    	public static int IMG_HEIGHT = 213;
    	public static int IMG_WIDTH = 320;
    	
    	private final static HashMap<Integer,Integer> colorMap = new HashMap<Integer,Integer>();
    	private final static String HDFS_COLORMAP = "cluster2color-map.txt";
    	
    	private final static ByteArrayOutputStream baos = new ByteArrayOutputStream();
        private static ObjectOutputStream os = null;
        
        private final static BytesWritable imgFileWrite = new BytesWritable();
    	
    	 @Override
 	    protected void setup(Context context) throws IOException, InterruptedException {

    		 FileSystem fs = FileSystem.get(context.getConfiguration());
    		 FSDataInputStream stream = fs.open(new Path(HDFS_COLORMAP));
 	    	 
    		 BufferedReader buf = new BufferedReader(new InputStreamReader(stream));
 	    	
    		 String line = "";
    		 while((line = buf.readLine()) != null) {
    			 String[] tokens = line.substring(1, line.length()-1).split(",");
                 
                 if (tokens.length == 2) {
                     if (!tokens[0].isEmpty() && !tokens[1].isEmpty())
                 	    colorMap.put(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
                 }
             }

    		 buf.close();
    		 
    		 os = new ObjectOutputStream(baos);
 	    }
    	
        @Override
        public void reduce(IntWritable key, Iterable<TripleOfInts> values, Context context) 
                throws IOException, InterruptedException {
            
            Iterator<TripleOfInts> iter = values.iterator();
            
            BufferedImage img = new BufferedImage(IMG_WIDTH, IMG_HEIGHT, BufferedImage.TYPE_INT_ARGB);
            
            while (iter.hasNext()) {
            	TripleOfInts pixel = iter.next();
            	Integer mappedValue = colorMap.get(pixel.getRightElement());
            			
            	// get the color map
            	if (mappedValue != null) {
            		img.setRGB(pixel.getLeftElement(), pixel.getMiddleElement(), mappedValue);
            	}
            	else {
            		img.setRGB(pixel.getLeftElement(), pixel.getMiddleElement(), 0);
            	}
            }
            
            // create BytesWritable
            os.reset();
            os.writeObject(img);
            imgFileWrite.set(baos.toByteArray(), 0, baos.size());
            context.write(key, imgFileWrite);
        }
        
        @Override
        public void cleanup(Context context) throws IOException {
        	baos.close();
        	os.close();
        }
    
    }
    
    private static final Logger LOG = Logger.getLogger(Pixels2Images.class);
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";
    
    @SuppressWarnings({ "static-access" })
    @Override
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

        if (!cmdline.hasOption(INPUT) || 
                !cmdline.hasOption(OUTPUT)) {
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

        LOG.info("Tool: " + Pixels2Images.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        
        job.setJobName(Pixels2Images.class.getSimpleName());
        job.setJarByClass(Pixels2Images.class);

        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job,
            SequenceFile.CompressionType.BLOCK);
        
        job.setOutputKeyClass(TripleOfInts.class);
        job.setOutputValueClass(IntWritable.class);

       
        job.setMapperClass(Pixels2ImMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TripleOfInts.class);
        
        job.setReducerClass(Pixels2ImReducer.class);
        
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
          
          ToolRunner.run(new Pixels2Images(), args);
      }
}
