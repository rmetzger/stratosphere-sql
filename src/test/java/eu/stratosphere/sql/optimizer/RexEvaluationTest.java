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
	public void trivialProj() {
		SqlTestResult result = test.execute("SELECT depName FROM departments");
		result.expectRowcount(3);
		result.expectColumn(0, ImmutableList.of("Sales", "Marketing", "Accounts") );
	}
	
	@Test
	public void trivialMulti() {
		SqlTestResult result = test.execute("SELECT depName, depName, depName FROM departments");
		result.expectRowcount(3);
		ImmutableList<String> exp = ImmutableList.of("Sales", "Marketing", "Accounts");
		result.expectColumn(0, exp);
		result.expectColumn(1, exp);
		result.expectColumn(2, exp);
	}
	
	@Test
	public void someTrivial() {
		SqlTestResult result = test.execute("SELECT depName, depName, UPPER(depName) FROM departments");
		result.expectRowcount(3);
		result.expectColumn(0, ImmutableList.of("Sales", "Marketing", "Accounts"));
		result.expectColumn(1, ImmutableList.of("Sales", "Marketing", "Accounts"));
		result.expectColumn(2, ImmutableList.of("SALES", "MARKETING", "ACCOUNTS"));
	}
	
	@Test
	public void substring() {
		SqlTestResult result = test.execute("SELECT SUBSTRING(depName FROM 1 FOR 2) FROM departments");
		result.expectRowcount(3);
		result.expectColumn(0, ImmutableList.of("Sa", "Ma", "Ac") );
	}
	
	@Test
	public void twoExpressionsSubString() {
		SqlTestResult result = test.execute("SELECT SUBSTRING(depName FROM 1 FOR 2), "
				+ "SUBSTRING(UPPER(depName) FROM 1 FOR 4), "
				+ "CASE SUBSTRING(depName FROM 1 FOR 2) "
				+ "WHEN 'Sa' THEN 'Salez' "
				+ "WHEN 'Ma' THEN 'Benchmarketing' "
				+ "ELSE 'unknown' "
				+ "END  "
				+ "FROM departments");
		result.expectRowcount(3);
		result.expectRow(0, ImmutableList.of("Sa", "SALE", "Salez"));
		result.expectRow(1, ImmutableList.of("Ma", "MARK", "Benchmarketing"));
		result.expectRow(2, ImmutableList.of("Ac", "ACCO", "unknown"));
	}
	
	/**
	 * Test if one function can access an argument two times
	 */
	@Test
	public void concat() {
		SqlTestResult result = test.execute("SELECT depName || depName FROM departments");
		result.expectRowcount(3);
		result.expectRow(0, ImmutableList.of("SalesSales" ));
		result.expectRow(1, ImmutableList.of("MarketingMarketing"));
		result.expectRow(2, ImmutableList.of("AccountsAccounts"));
	}
	
	@Test
	public void concatDifferent() {
		SqlTestResult result = test.execute("SELECT depName || depManager  FROM departments");
		result.expectRowcount(3);
		result.expectRow(0, ImmutableList.of("SalesJohn" ));
		result.expectRow(1, ImmutableList.of("MarketingPete"));
		result.expectRow(2, ImmutableList.of("AccountsClaus"));
	}
	@Test
	public void concatWithStatic() {
		SqlTestResult result = test.execute("SELECT depName || ' is managed by ' || depManager  FROM departments");
		result.expectRowcount(3);
		result.expectRow(0, ImmutableList.of("Sales is managed by John" ));
		result.expectRow(1, ImmutableList.of("Marketing is managed by Pete"));
		result.expectRow(2, ImmutableList.of("Accounts is managed by Claus"));
	}
}
