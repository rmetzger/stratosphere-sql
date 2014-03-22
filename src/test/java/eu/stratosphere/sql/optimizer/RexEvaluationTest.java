package eu.stratosphere.sql.optimizer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
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
	private static SqlTest test;
	private Object monitor = new Object();

	
	@Before public void start() {
		synchronized (monitor) {
			test = new SqlTest(SqlTestTable.Tbl);
		}
		
	}
	@After public void stop() {
		synchronized (monitor) {
			test.close();
			test = new SqlTest(SqlTestTable.Tbl);
		}
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
		result.expectRowcount(3);
		result.expectColumn(0, ImmutableList.of("Sales", "Marketing", "Acccounts") );
	}
	
	@Test
	public void substring() {
		SqlTestResult result = test.execute("SELECT SUBSTRING(customerName FROM 1 FOR 2) FROM tbl");
		result.expectRowcount(3);
		result.expectColumn(0, ImmutableList.of("Sa", "Ma", "Ac") );
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
		result.expectRowcount(3);
		result.expectRow(0, ImmutableList.of("Sa", "SALE", "Salez"));
		result.expectRow(1, ImmutableList.of("Ma", "MARK", "Benchmarketing"));
		result.expectRow(2, ImmutableList.of("Ac", "ACCO", "unknown"));
	}
}
