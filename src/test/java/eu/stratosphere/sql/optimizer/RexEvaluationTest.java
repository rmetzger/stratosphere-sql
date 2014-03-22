package eu.stratosphere.sql.optimizer;

import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.commons.jexl2.JexlEngine;
import org.eigenbase.sql.parser.SqlParseException;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import eu.stratosphere.sql.optimizer.SqlTest.SqlTestResult;
import eu.stratosphere.sql.optimizer.SqlTest.SqlTestTable;

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
		SqlTest test = new SqlTest(SqlTestTable.Tbl);
		SqlTestResult result = test.execute("SELECT SUBSTRING(customerName FROM 1 FOR 2) FROM tbl");
		result.expectRow(2, ImmutableSet.of("Sa", "Ma", "Ac") );
		
//		String sql = "SELECT SUBSTRING(customerName FROM 1 FOR 2) FROM tbl";
//		Launcher l = Launcher.getInstance();
//		l.convertSQLToPlan(sql);
	}
}
