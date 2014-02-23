package eu.stratosphere.sql.optimizer;

import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.sql.Launcher;
import eu.stratosphere.sql.relOpt.StratosphereSqlFilter.StratosphereSqlFilterMapOperator;

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
		"SELECT customerName, customerId, customerId, customerId FROM tbl WHERE customerId = 2"
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
