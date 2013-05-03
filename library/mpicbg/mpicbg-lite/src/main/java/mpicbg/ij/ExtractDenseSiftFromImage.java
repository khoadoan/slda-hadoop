package mpicbg.ij;

import ij.ImagePlus;
import ij.process.ImageProcessor;

import java.awt.image.BufferedImage;
import java.io.DataInputStream;
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
			
			ImagePlus iplus = new ImagePlus("input", input);
			
			final ImageProcessor ip1 = iplus.getProcessor().convertToFloat();
			iplus.getProcessor().duplicate().convertToRGB();
			
			final DenseSIFT ijSift = new DenseSIFT( new FloatArray2DSIFT( p ) );
			ijSift.setStepSize(stepSize);
			fs.clear();
			
			ijSift.extractFeatures( ip1, fs );
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
    public ExtractDenseSiftFromImage(DataInputStream inStream, int stepSize, int blockSize) {
		
	    // Load the image
		try {
			
			BufferedImage input;
			input = ImageIO.read(inStream);
			
			ImagePlus iplus = new ImagePlus("input", input);
			
			final ImageProcessor ip1 = iplus.getProcessor().convertToFloat();
			iplus.getProcessor().duplicate().convertToRGB();
			
			p.fdSize = blockSize;
			final DenseSIFT ijSift = new DenseSIFT( new FloatArray2DSIFT( p ) );
			ijSift.setStepSize(stepSize);
			fs.clear();
			
			ijSift.extractFeatures( ip1, fs );
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public List<Feature> getExtractedFeatures() {
		return fs;
	}
	
}
