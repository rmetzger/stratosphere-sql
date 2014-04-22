package eu.stratosphere.sql.optimizer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import eu.stratosphere.sql.optimizer.SqlTest.SqlTestResult;
import eu.stratosphere.sql.optimizer.SqlTest.SqlTestTable;

/**
 * Test the different schema adapters
 *
 */
public class SchemaTests {
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
	public void avroSchema() {
		SqlTestResult result = test.execute("SELECT SID, NAME FROM CUSTOM_TABLE");
		result.expectRowcount(5);
		result.expectRow(0, ImmutableList.of(1, "Rohan") );
		result.expectRow(4, ImmutableList.of(5, "Stephan") );
	}


}
