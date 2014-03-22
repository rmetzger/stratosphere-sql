package eu.stratosphere.sql.optimizer;

import org.junit.Before;
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
	private SqlTest test;
	@Before
	public void prepare() {
		test = new SqlTest(SqlTestTable.Tbl);
	}
	
	//
	// The Rex' below are evaluated in the projection operator
	//
	
	/**
	 * Test if the fast shortcut without calling generated code is also working.
	 */
	@Test
	public void noEval() {
		SqlTestResult result = test.execute("SELECT customerName FROM tbl");
		result.expectRow(0, ImmutableSet.of("Sales", "Marketing", "Acccounts") );
	}
	
	@Test
	public void substring() {
		SqlTestResult result = test.execute("SELECT SUBSTRING(customerName FROM 1 FOR 2) FROM tbl");
		result.expectRow(0, ImmutableSet.of("Sa", "Ma", "Ac") );
	}
	
	@Test
	public void twoExpressionsSubString() {
		SqlTestResult result = test.execute("SELECT SUBSTRING(customerName FROM 1 FOR 2), "
				+ "SUBSTRING(UPPER(customerName) FROM 1 FOR 4), "
				+ "CASE SUBSTRING(customerName FROM 1 FOR 2) "
				+ "WHEN 'Sa' THEN 'Salez' "
				+ "WHEN 'Ma' THEN 'Benchmarketing' "
				+ "ELSE 'unknown' "
				+ "END  "
				+ "FROM tbl");
		result.expectRow(0, ImmutableSet.of("Sa", "Ma", "Ac") );
		result.expectRow(1, ImmutableSet.of("SALE", "MARK", "ACCO") );
		result.expectRow(2, ImmutableSet.of("Salez", "Benchmarketing", "unknown") );
	}
}
