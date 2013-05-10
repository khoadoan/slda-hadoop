
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.imageio.ImageIO;

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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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

public class Images2Pixels extends Configured implements Tool {
	
	/**
	 * The mapper class which takes a sequence file of combined
	 * images and produces another sequence file of mapped 
	 * pixels value. This class is to be used in map only job.
	 * The output is not sorted or grouped.
	 * @author Rahmatri Mardiko
	 */
	private static class Im2PixelsMapperOnly extends 
	    Mapper<IntWritable, BytesWritable, TripleOfInts, IntWritable> {

		private int count;
		private final static HashMap<Integer,Integer> colorMap = new HashMap<Integer,Integer>();
		
		private final static String HDFS_COLORMAP = "color-map.txt";
		
	    private final static IntWritable VALUE = new IntWritable();
	    private final static TripleOfInts KEY = new TripleOfInts();

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	count = 0;
	    	
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
	    }
	    
	    @Override
	    public void map(IntWritable key, BytesWritable file, Context context) throws IOException,
	        InterruptedException {
	    	
	    	BufferedImage img = ImageIO.read(new ByteArrayInputStream(file.getBytes()));
	    	for (int x = 0; x < img.getWidth(); x++) {
	    		for (int y = 0; y < img.getHeight(); y++) {
	    			
	    			KEY.set(count++, x, y);
	    			
	    			int rgb = img.getRGB(x, y);
	    			if (!colorMap.containsKey(rgb)) {
	    				int value = colorMap.size();
	    				colorMap.put(rgb, value);
	    			}
	    			
	    			VALUE.set(colorMap.get(rgb));
	    			
	    			context.write(KEY, VALUE);
	    		}
	    	}

	    }
	}
	
	/**
	 * The mapper class which takes a sequence file of combined
	 * images and produces pairs of <mapped rgb value, pixel location>.
	 * This class is to be used with reducer where the output is grouped
	 * and sorted by the mapped rgb value.
	 * @author Rahmatri Mardiko
	 */
	private static class Im2PixelsMapper extends Mapper<Text, BytesWritable, IntWritable, TripleOfInts> {

		private int count;
		private final static HashMap<Integer,Integer> colorMap = new HashMap<Integer,Integer>();
		
		private final static String HDFS_COLORMAP = "color-map.txt";
		
	    private final static IntWritable KEY = new IntWritable();
	    private final static TripleOfInts VALUE = new TripleOfInts();

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	count = 0;
	    	
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
	    }
	    
	    @Override
	    public void map(Text key, BytesWritable file, Context context) throws IOException,
	        InterruptedException {
	    	
	    	BufferedImage img = ImageIO.read(new ByteArrayInputStream(file.getBytes()));
	    	for (int x = 0; x < img.getWidth(); x++) {
	    		for (int y = 0; y < img.getHeight(); y++) {
	    			
	    			VALUE.set(count++, x, y);
	    			
	    			int rgb = img.getRGB(x, y);
	    			if (!colorMap.containsKey(rgb)) {
	    				int value = colorMap.size();
	    				colorMap.put(rgb, value);
	    			}
	    			
	    			KEY.set(colorMap.get(rgb));
	    			
	    			context.write(KEY, VALUE);
	    		}
	    	}

	    }
	}
	
	/**
	 * The reducer class which groups together pixels that map to the
	 * same value
	 * @author Rahmatri Mardiko
	 *
	 */
	private static class Im2PixelsReducer extends Reducer
	    <IntWritable, TripleOfInts, TripleOfInts, IntWritable> {
		
		@Override
	    public void reduce(IntWritable key, Iterable<TripleOfInts> values, Context context) 
	    		throws IOException, InterruptedException {
			
			Iterator<TripleOfInts> iter = values.iterator();
			
			// producing combined sequence file
			while (iter.hasNext()) {
				context.write(iter.next(), key);
			}
		}
		
	}
	
	private static final Logger LOG = Logger.getLogger(Images2Pixels.class);
	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String NUM_REDUCERS = "numReducers";
	private static final String BUILD_MAP_DIR = "buildMap";
	private final static String HDFS_COLORMAP = "color-map.txt";
	
	@SuppressWarnings({ "static-access" })
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();

	    options.addOption(OptionBuilder.withArgName("path").hasArg()
	        .withDescription("input path").create(INPUT));
	    options.addOption(OptionBuilder.withArgName("path").hasArg()
	        .withDescription("output path").create(OUTPUT));
	    options.addOption(OptionBuilder.withArgName("path").hasArg()
		        .withDescription("build map directory").create(BUILD_MAP_DIR));
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
	    		!cmdline.hasOption(OUTPUT) || 
	    		!cmdline.hasOption(BUILD_MAP_DIR)) {
	      System.out.println("args: " + Arrays.toString(args));
	      HelpFormatter formatter = new HelpFormatter();
	      formatter.setWidth(120);
	      formatter.printHelp(this.getClass().getName(), options);
	      ToolRunner.printGenericCommandUsage(System.out);
	      return -1;
	    }

	    String inputPath = cmdline.getOptionValue(INPUT);
	    String outputPath = cmdline.getOptionValue(OUTPUT);
	    String buildMapDir = cmdline.getOptionValue(BUILD_MAP_DIR);
	    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
	        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

	    LOG.info("Tool: " + Images2Pixels.class.getSimpleName());
	    LOG.info(" - input path: " + inputPath);
	    LOG.info(" - output path: " + outputPath);
	    LOG.info(" - build map directory: " + buildMapDir);
	    LOG.info(" - number of reducers: " + reduceTasks);

	    Configuration conf = getConf();
	    Job job = Job.getInstance(conf);
	    
	    HashMap<Integer,Integer> myColorMap = (new ColorMapGT(buildMapDir, conf)).getColorMap();
	    LOG.info("map size: " + myColorMap.size());
	    
	    // write the hashmap to textfile
	    FileSystem fs = FileSystem.get(conf);
	    Path path = new Path(HDFS_COLORMAP);
	    FSDataOutputStream out = fs.create(path);
	    BufferedWriter buf = new BufferedWriter(new OutputStreamWriter(out));
	    for (Entry<Integer,Integer> e : myColorMap.entrySet()) {
	    	buf.write("(" + e.getKey() + "," + e.getValue() + ")\n");
	    }
	    out.close();
	    buf.close();
	    
	    // put the file on hdfs
	    fs.copyFromLocalFile(false, true, path, path);
	    	    
	    job.setJobName(Images2Pixels.class.getSimpleName());
	    job.setJarByClass(Images2Pixels.class);

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

	    if (reduceTasks > 0) {
	    	job.setMapperClass(Im2PixelsMapper.class);
	    	job.setMapOutputKeyClass(IntWritable.class);
	    	job.setMapOutputValueClass(TripleOfInts.class);
		    
	    	job.setReducerClass(Im2PixelsReducer.class);
	    }
	    else {
	    	job.setMapperClass(Im2PixelsMapperOnly.class);
	    }
	    
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
		  
		  ToolRunner.run(new Images2Pixels(), args);
	  }
}
