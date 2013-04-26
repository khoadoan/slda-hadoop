package mpicbg.ij;

import java.awt.Color;
import java.awt.Polygon;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import mpicbg.imagefeatures.Feature;
import mpicbg.imagefeatures.FloatArray2DSIFT;

import org.junit.Test;

import ij.ImagePlus;
import ij.process.ImageProcessor;

public class TestRunSIFT {
	
	final static private FloatArray2DSIFT.Param p = new FloatArray2DSIFT.Param();
	final static List< Feature > fs = new ArrayList< Feature >();

	static void drawSquare( ImageProcessor ip, double[] o, double scale, double orient )
	{
		scale /= 2;
		
	    double sin = Math.sin( orient );
	    double cos = Math.cos( orient );
	    
	    int[] x = new int[ 6 ];
	    int[] y = new int[ 6 ];
	    

	    x[ 0 ] = ( int )( o[ 0 ] + ( sin - cos ) * scale );
	    y[ 0 ] = ( int )( o[ 1 ] - ( sin + cos ) * scale );
	    
	    x[ 1 ] = ( int )o[ 0 ];
	    y[ 1 ] = ( int )o[ 1 ];
	    
	    x[ 2 ] = ( int )( o[ 0 ] + ( sin + cos ) * scale );
	    y[ 2 ] = ( int )( o[ 1 ] + ( sin - cos ) * scale );
	    x[ 3 ] = ( int )( o[ 0 ] - ( sin - cos ) * scale );
	    y[ 3 ] = ( int )( o[ 1 ] + ( sin + cos ) * scale );
	    x[ 4 ] = ( int )( o[ 0 ] - ( sin + cos ) * scale );
	    y[ 4 ] = ( int )( o[ 1 ] - ( sin - cos ) * scale );
	    x[ 5 ] = x[ 0 ];
	    y[ 5 ] = y[ 0 ];
	    
	    ip.drawPolygon( new Polygon( x, y, x.length ) );
	}
	
	
	static void drawSquare2( ImageProcessor ip, double[] o, double scale, double orient )
	{
		scale /= 2;
		
	    double sin = Math.sin( orient );
	    double cos = Math.cos( orient );
	    
	    int[] x = new int[ 5 ];
	    int[] y = new int[ 5 ];
	    

	    x[ 0 ] = ( int )( o[ 0 ] + ( sin - cos ) * scale );
	    y[ 0 ] = ( int )( o[ 1 ] - ( sin + cos ) * scale );
	    
	    x[ 1 ] = ( int )( o[ 0 ] + ( sin + cos ) * scale );
	    y[ 1 ] = ( int )( o[ 1 ] + ( sin - cos ) * scale );
	    x[ 2 ] = ( int )( o[ 0 ] - ( sin - cos ) * scale );
	    y[ 2 ] = ( int )( o[ 1 ] + ( sin + cos ) * scale );
	    x[ 3 ] = ( int )( o[ 0 ] - ( sin + cos ) * scale );
	    y[ 3 ] = ( int )( o[ 1 ] - ( sin - cos ) * scale );
	    x[ 4 ] = x[ 0 ];
	    y[ 4 ] = y[ 0 ];
	    
	    ip.drawPolygon( new Polygon( x, y, x.length ) );
	}
	
	@Test
	public void testBasic() throws IOException {
		
		// Load the image
		BufferedImage input = ImageIO.read(new File("images/test03.bmp"));
		
		ImagePlus iplus = new ImagePlus("test", input);
		
		//final GenericDialog gd = new GenericDialog( "Test SIFT" );
		
		final ImageProcessor ip1 = iplus.getProcessor().convertToFloat();
		final ImageProcessor ip2 = iplus.getProcessor().duplicate().convertToRGB();
		
		final DenseSIFT ijSift = new DenseSIFT( new FloatArray2DSIFT( p ) );
		fs.clear();
		
		final long start_time = System.currentTimeMillis();
		System.out.print( "processing SIFT ..." );
		ijSift.extractFeatures( ip1, fs );
		System.out.println( " took " + ( System.currentTimeMillis() - start_time ) + "ms" );
		
		System.out.println( fs.size() + " features identified and processed" );
		
		ip2.setLineWidth( 1 );
		ip2.setColor( Color.red );
		for ( final Feature f : fs ) {
			drawSquare( ip2, new double[]{ f.location[ 0 ], f.location[ 1 ] }, p.fdSize * 2.0 * ( double )f.scale, ( double )f.orientation );
			//drawSquare2( ip2, new double[]{ f.location[ 0 ], f.location[ 1 ] }, p.fdSize * 2.5 * ( double )f.scale, ( double )f.orientation );
		}
		
		ImageIO.write(ip2.getBufferedImage(),"png",new File("images/ouput.png"));
		
	}

}
