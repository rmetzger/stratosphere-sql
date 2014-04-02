package eu.stratosphere.sql.utils;

import junit.framework.Assert;
import net.hydromatic.optiq.DataContext;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import eu.stratosphere.sql.relOpt.StratosphereDataContext;

@RunWith(JUnit4.class)
public class StratosphereDataContextTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testBounds() {
		DataContext ctx = new StratosphereDataContext();
		Assert.assertNull( ( (Object[]) ctx.get(StratosphereDataContext.REC_FIELD))[0]);

		exception.expect(IndexOutOfBoundsException.class);
        Object d = ( (Object[]) ctx.get(StratosphereDataContext.REC_FIELD))[1];
	}
	
	@Test
	public void testLower() {
		DataContext ctx = new StratosphereDataContext();
		exception.expect(IndexOutOfBoundsException.class);
        Object d = ( (Object[]) ctx.get(StratosphereDataContext.REC_FIELD))[-1];
	}
	
	@Test
	public void testExpansion() {
		StratosphereDataContext ctx = new StratosphereDataContext();
		ctx.set(1, "hello");
		Assert.assertEquals(( (Object[]) ctx.get(StratosphereDataContext.REC_FIELD))[1], "hello");
		exception.expect(IndexOutOfBoundsException.class);
		Object d  = ( (Object[]) ctx.get(StratosphereDataContext.REC_FIELD))[2];
	}
	
	@Test
	public void testExpansion2() {
		StratosphereDataContext ctx = new StratosphereDataContext();
		ctx.set(1, "hello");
		Assert.assertEquals(( (Object[]) ctx.get(StratosphereDataContext.REC_FIELD))[1], "hello");
		ctx.set(245, "hello2");
		Assert.assertEquals(( (Object[]) ctx.get(StratosphereDataContext.REC_FIELD))[120], null);
		Assert.assertEquals(( (Object[]) ctx.get(StratosphereDataContext.REC_FIELD))[245], "hello2");
		Assert.assertEquals(( (Object[]) ctx.get(StratosphereDataContext.REC_FIELD))[1], "hello"); // correct copy
		ctx.set(255, "hello3");
		ctx.set(1000, "hello4");
		exception.expect(RuntimeException.class);
		ctx.set(1200, "hello5");
	}
	
}
