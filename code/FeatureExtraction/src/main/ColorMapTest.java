import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;


public class ColorMapTest {

	private ColorMapGT map;
	
	@Before
	public void initMap() {
		map = new ColorMapGT();
	}
	
	@Test
	public void testColorMapGT() {
		assertTrue(map.getSize() > 0);
	}

	@Test
	public void testGetSize() {
		fail("Not yet implemented");
	}

	@Test
	public void testGet() {
		assertTrue(map.get(-8355840) == 8);
	}

}
