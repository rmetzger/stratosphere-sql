package eu.stratosphere.sql.optimizer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import eu.stratosphere.sql.optimizer.SqlTest.SqlTestResult;
import eu.stratosphere.sql.optimizer.SqlTest.SqlTestTable;

/**
 * Test the base operators
 *
 */
public class OperatorsTest {
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
	
	@Test
	public void countStar() {
		SqlTestResult result = test.execute("SELECT COUNT(*) FROM departments");
		result.expectRowcount(1);
		result.expectRow(0, ImmutableList.of(3L) );
	}
	
	@Test
	public void countAndSumStar() {
		SqlTestResult result = test.execute("SELECT COUNT(*), SUM(depNo) FROM departments");
		result.expectRowcount(1);
		result.expectRow(0, ImmutableList.of(3L, 60) );
	}
	
	@Test
	public void countAndSumStarInGroups() {
		SqlTestResult result = test.execute("SELECT COUNT(*), SUM(depNo) "
				+ "FROM departments "
				+ "GROUP BY depNo");
		result.expectRowcount(3);
		result.expectRow(0, ImmutableList.of(1L, 10) );
		result.expectRow(1, ImmutableList.of(1L, 20) );
		result.expectRow(2, ImmutableList.of(1L, 30) );
	}
	
	@Test
	public void join() {
		SqlTestResult result = test.execute("SELECT * "
				+ "FROM departments, customer "
				+ "WHERE departments.depNo/10 = customer.customerId");
		result.expectRowcount(3);
		result.expectRow(0, ImmutableList.of(1L, 10) );
		result.expectRow(1, ImmutableList.of(1L, 20) );
		result.expectRow(2, ImmutableList.of(1L, 30) );
	}
	
}
