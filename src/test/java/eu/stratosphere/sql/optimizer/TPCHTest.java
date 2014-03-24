package eu.stratosphere.sql.optimizer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import eu.stratosphere.sql.optimizer.SqlTest.SqlTestResult;
import eu.stratosphere.sql.optimizer.SqlTest.SqlTestTable;

public class TPCHTest {
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
		result.expectRow(0, ImmutableList.of(3) );
	}
	
	@Test
	public void queryOne() {
		SqlTestResult result = test.execute("SELECT \n" + 
				"l_returnflag, \n" + 
				"l_linestatus, \n" + 
				"sum(l_quantity) as sum_qty, \n" + 
				"sum(l_extendedprice) as sum_base_price, \n" + 
				"sum(l_extendedprice*(1-l_discount)) as sum_disc_price, \n" + 
				"sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, \n" + 
				"avg(l_quantity) as avg_qty, \n" + 
				"avg(l_extendedprice) as avg_price, \n" + 
				"avg(l_discount) as avg_disc, \n" + 
				"count(*) as count_order \n" + 
				"FROM \n" + 
				"lineitem \n" + 
				"WHERE \n" + 
				"l_shipdate <= date '1998-12-01' - interval '100' day (3) \n" + 
				"GROUP BY \n" + 
				"l_returnflag, \n" + 
				"l_linestatus \n" + 
				"ORDER BY \n" + 
				"l_returnflag, \n" + 
				"l_linestatus");
		result.expectRowcount(1);
		result.expectRow(0, ImmutableList.of(3L, 60L) );
	}
}
