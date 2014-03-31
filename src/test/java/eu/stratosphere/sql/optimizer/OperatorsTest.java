package eu.stratosphere.sql.optimizer;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
	public void countAndSum() {
		SqlTestResult result = test.execute("SELECT COUNT(*), SUM(depNo) FROM departments");
		result.expectRowcount(1);
		result.expectRow(0, ImmutableList.of(3L, 60) );
	}

	@Ignore
	@Test
	public void countSumAvg() {
		SqlTestResult result = test.execute("SELECT COUNT(*), SUM(depNo), AVG(1+(depNo*10)) FROM departments");
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


	/**
	 * Boils (currently) down to a full cartesian product with a filter
	 *
	 * 30:StratosphereSqlProjection(depNo=[$0], depName=[$1], depManager=[$2], customerId=[$3], customerName=[$4]): rowcount = 1500.0, cumulative cost = {3200.0 rows, 7702.0 cpu, 0.0 io}
	 *  29:StratosphereSqlJoin(condition=[=(/($0, 10), $3)], joinType=[inner]): rowcount = 1500.0, cumulative cost = {1700.0 rows, 202.0 cpu, 0.0 io}
	 *    0:CSVStratosphereDataSource(table=[[mySchema, departments]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}
	 *    1:CSVStratosphereDataSource(table=[[mySchema, customer]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}
	 */
	@Test
	public void join() {
		SqlTestResult result = test.execute("SELECT * "
				+ "FROM departments, customer "
				+ "WHERE departments.depNo/10 = customer.customerId");
		result.expectRowcount(3);
		result.expectRow(0, ImmutableList.of(10, "Sales", "John", 1, "Hanspeter") );
		result.expectRow(1, ImmutableList.of(20, "Marketing", "Pete", 2, "Ottomayer") );
		result.expectRow(2, ImmutableList.of(30, "Accounts", "Claus", 3, "Eric") );
	}

}
