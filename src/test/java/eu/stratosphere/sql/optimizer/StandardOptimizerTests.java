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
//	+ "FROM (SELECT COUNT(*) AS cnt FROM tbl GROUP BY NAME) AS a, tbl	"
//	+ "WHERE a.cnt = tbl.DEPTNO "
//	+ "ORDER BY a.cnt ASC LIMIT 2";
	private static String[] queries = {
		"SELECT * FROM departments",
		"SELECT depName, depNo, depNo, depNo FROM departments",
		"SELECT depName, depNo, depNo, depNo FROM departments WHERE depNo = 2",
		"SELECT depNo FROM departments WHERE ( depNo = 2 OR depNo = 3 OR depNo=3 ) AND (depNo < 15)",
		"SELECT a.depName, a.depNo, b.depNo FROM departments a, departments b WHERE (a.depNo = b.depNo) AND (a.depNo < 15)"
	};
	
	@Test
	public void executeQueries() {
		String sql = null;
		Launcher l = Launcher.getInstance();
		try {
			for(int i = 0; i < queries.length; i++) {
				sql = queries[i];
				Plan p = l.convertSQLToPlan(sql);
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Failed to optimize query "+sql+" with message "+e.getMessage());
		}
	}
}
