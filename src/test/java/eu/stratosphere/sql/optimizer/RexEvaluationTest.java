package eu.stratosphere.sql.optimizer;

import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.commons.jexl2.JexlEngine;
import org.eigenbase.sql.parser.SqlParseException;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.sql.Launcher;

/**
 * Test Rex evaluation using Optiq-based code gen.
 * 
 * Goal: Get projection running with Rex.
 *
 */
public class RexEvaluationTest {
	private static final JexlEngine jexl = new JexlEngine();
	static {
		jexl.setCache(512);
		jexl.setLenient(false);
		jexl.setSilent(false);
	}
	
	
	@Test
	public void doIt() throws SqlParseException, ValidationException, RelConversionException {
		String sql = "SELECT SUBSTRING('12345', 2) FROM tbl";
		Plan p = Launcher.convertSQLToPlan(sql);
		
		
	}
}
