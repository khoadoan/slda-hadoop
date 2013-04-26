package mpicbg.ij;

import ij.ImagePlus;
import ij.process.ImageProcessor;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;

public class ExtractDenseSiftFromImage {

	final static private FloatArray2DSIFT.Param p = new FloatArray2DSIFT.Param();
	final static List< Feature > fs = new ArrayList< Feature >();
	
	public ExtractDenseSiftFromImage(String filePath, int stepSize) {
		
	    // Load the image
		try {
			BufferedImage input;
			input = ImageIO.read(new File(filePath));
			
			ImagePlus iplus = new ImagePlus("test", input);
			
			final ImageProcessor ip1 = iplus.getProcessor().convertToFloat();
			final ImageProcessor ip2 = iplus.getProcessor().duplicate().convertToRGB();
			
			final DenseSIFT ijSift = new DenseSIFT( new FloatArray2DSIFT( p ) );
			fs.clear();
			
			final long start_time = System.currentTimeMillis();
			System.out.print( "processing SIFT ..." );
			ijSift.extractFeatures( ip1, fs );
			System.out.println( " took " + ( System.currentTimeMillis() - start_time ) + "ms" );
			
			System.out.println( fs.size() + " features identified and processed" );
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public float[][] getExtractedFeatures() {
		
		float[][] features = new float[fs.size()][fs.get(0).descriptor.length];
		
		int count = 0;
		for (final Feature f : fs) {
			features[count] = f.descriptor;
			count++;
		}
		
		return features;
	}
	
}
