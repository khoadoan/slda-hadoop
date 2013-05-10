import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class ColorMapGT {
	
	private HashMap<Integer,Integer> map;
	private final String INPUT_DIR = "C:/MSRC_ObjCategImageDatabase_v1";
	
	public ColorMapGT() {
		
		map = new HashMap<Integer,Integer>();
		
		File folder = new File(INPUT_DIR);
		File[] files = folder.listFiles();
		
		try {
			
			int count = 0;
			for (File f : files) {
				
				String name = f.getName();
				String suffix = name.substring(name.length()-7,name.length()-4);
				
				if (suffix.equalsIgnoreCase("_GT")) {
					BufferedImage img = ImageIO.read(f);
					for (int x = 0; x < img.getWidth(); x++) {
						for (int y = 0; y < img.getHeight(); y++) {
							int pixel = img.getRGB(x, y);
							if (!map.containsKey(pixel)) {
								map.put(pixel, count++);
							}
						}
					}
				}
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
	}
	
	/**
	 * 
	 * @param directory the directory of ground truth images
	 * @param conf the configuration of the job
	 * @throws IOException
	 */
	public ColorMapGT(String directory, Configuration conf) throws IOException {
		
		map = new HashMap<Integer,Integer>();
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(new Path(directory));
		
		try {
			
			int count = 0;
			for (FileStatus f : files) {
				
				BufferedImage img = ImageIO.read(fs.open(f.getPath()));
				for (int x = 0; x < img.getWidth(); x++) {
					for (int y = 0; y < img.getHeight(); y++) {
						int pixel = img.getRGB(x, y);
						if (!map.containsKey(pixel)) {
							map.put(pixel, count++);
						}
					}
				}
				
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	public HashMap<Integer,Integer> getColorMap() {
		return new HashMap<Integer,Integer>(map);
	}
	
	public int getSize() {
		return map.size();
	}
	
	public int get(int key) {
		return map.get(key);
	}
	
	/*
	public boolean put(int key, int value) {
		
		if (map.containsKey(key)) {
			return false;
		}
		
		map.put(key, value);
		return true;
	}*/
	
}
