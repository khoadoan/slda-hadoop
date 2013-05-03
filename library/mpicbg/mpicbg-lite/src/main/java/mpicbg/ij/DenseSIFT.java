package mpicbg.ij;

import ij.process.FloatProcessor;
import ij.process.ImageProcessor;

import java.util.Collection;

import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.Filter;
import mpicbg.imagefeatures.FloatArray2D;
import mpicbg.imagefeatures.FloatArray2DSIFT;
import mpicbg.imagefeatures.FloatArray2DScaleOctave;
import mpicbg.imagefeatures.ImageArrayConverter;

/**
 * 
 * @author Stephan Saalfeld <saalfeld@mpi-cbg.de>
 * @version 0.4b
 */
public class DenseSIFT extends FeatureTransform< FloatArray2DSIFT >
{
	/**
	 * Constructor
	 * 
	 * @param t feature transformation
	 */
	public DenseSIFT( final FloatArray2DSIFT t )
	{
		super( t );
	}
	
	private int stepSize = 16;
	public void setStepSize(int size) {
		stepSize = size;
	}

	
	/**
	 * Extract SIFT features from an ImageProcessor
	 * 
	 * @param ip
	 * @param features the list to be filled
	 * 
	 * @return number of detected features
	 */
	@Override
	final public void extractFeatures( final ImageProcessor ip, final Collection< Feature > features )
	{
		/* make sure that integer rounding does not result in an image of t.getMaxOctaveSize() + 1 */
		final float maxSize = t.getMaxOctaveSize() - 1;
		float scale = 1.0f;
		FloatArray2D fa;
		if ( maxSize < ip.getWidth() || maxSize < ip.getHeight() )
		{
			/* scale the image respectively */
			scale = ( float )Math.min( maxSize / ip.getWidth(), maxSize / ip.getHeight() );
			final FloatProcessor fp = ( FloatProcessor )ip.convertToFloat();
			fp.setMinAndMax( ip.getMin(), ip.getMax() );
			final FloatProcessor ipScaled = mpicbg.ij.util.Filter.createDownsampled( fp, scale, 0.5f, 0.5f );
			fa = new FloatArray2D( ipScaled.getWidth(), ipScaled.getHeight() );
			ImageArrayConverter.imageProcessorToFloatArray2DCropAndNormalize( ipScaled, fa );
		}
		else
		{
			fa = new FloatArray2D( ip.getWidth(), ip.getHeight() );
			ImageArrayConverter.imageProcessorToFloatArray2DCropAndNormalize( ip, fa );
		}
		
		final float[] initialKernel;
		
		final float initialSigma = t.getInitialSigma();
		if ( initialSigma < 1.0 )
		{
			scale *= 2.0f;
			t.setInitialSigma( initialSigma * 2 );
			final FloatArray2D fat = new FloatArray2D( fa.width * 2 - 1, fa.height * 2 - 1 ); 
			FloatArray2DScaleOctave.upsample( fa, fat );
			
			fa = fat;
			initialKernel = Filter.createGaussianKernel( ( float )Math.sqrt( t.getInitialSigma() * t.getInitialSigma() - 1.0 ), true );
		}
		else
			initialKernel = Filter.createGaussianKernel( ( float )Math.sqrt( initialSigma * initialSigma - 0.25 ), true );
		
		fa = Filter.convolveSeparable( fa, initialKernel, initialKernel );
		t.init( fa );
		t.extractDenseFeatures(stepSize, ip.getWidth(), ip.getHeight(), features);
		if ( scale != 1.0f )
		{
			for ( Feature f : features )
			{
				f.scale /= scale;
				f.location[ 0 ] /= scale;
				f.location[ 1 ] /= scale;
			}
			t.setInitialSigma( initialSigma );
		}	
	} 
}
