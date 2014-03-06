package eu.stratosphere.sql.optimizer;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.sql.Launcher;

/**
 * Test if Optiq is able to parse
 * a set of SQL queries.
 *
 */
public class StandardOptimizerTests {
	
//	String sql = "SELECT a.cnt "
//	+ "FROM (SELECT COUNT(*) AS cnt FROM tbl GROUP BY NAME) AS a, tbl  "
//	+ "WHERE a.cnt = tbl.DEPTNO "
//	+ "ORDER BY a.cnt ASC LIMIT 2";
	private static String[] queries = {
		"SELECT * FROM tbl",
		"SELECT customerName, customerId, customerId, customerId FROM tbl",
		"SELECT customerName, customerId, customerId, customerId FROM tbl WHERE customerId = 2",
		"SELECT customerId FROM tbl WHERE ( customerId = 2 OR customerId = 3 OR customerId=3 ) AND (customerId < 15)",
		"SELECT a.customerName, a.customerId, b.customerId FROM tbl a, tbl b WHERE (a.customerId = b.customerId) AND (a.customerId < 15)"
	};
	
	@Test
	public void executeQueries() {
		String sql = null;
		try {
			for(int i = 0; i < queries.length; i++) {
				sql = queries[i];
				Plan p = Launcher.convertSQLToPlan(sql);
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Failed to optimize query "+sql+" with message "+e.getMessage());
		}
	}
}
