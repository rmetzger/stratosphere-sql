package eu.stratosphere.sql.optimizer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.sql.Launcher;
import eu.stratosphere.sql.Launcher.Pair;
import eu.stratosphere.sql.relOpt.StratosphereRelUtils;
import eu.stratosphere.types.JavaValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;

/**
 * Base class for Stratosphere SQL tests.
 * 
 * TODO: finish implementation
 * TODO: replace by JDBC calling.
 */
public class SqlTest {
	// predefined test tables.
	public enum SqlTestTable {
		Tbl
	}
	
	public static class SqlTestResult {
		Collection<Record> result;
		public SqlTestResult(Collection<Record> result) {
			Preconditions.checkNotNull(result);
			this.result = result;
		}

		public void expectRowcount(int cnt) {
			Assert.assertEquals(cnt, result.size());
		}
		
		/**
		 * Test if row rowId contains the elements in the set.
		 * @param rowId
		 * @param elements
		 */
		public void expectRow(int rowId, Set<?> elements) {
			Set<?> copy = new HashSet(elements);
			Class firstType = null;
			for(Object o : elements) {
				if(firstType == null) {
					firstType = o.getClass();
					break;
				}
				if(o.getClass() != firstType) {
					throw new RuntimeException("All elements in the expected set have to have the same type");
				}
			}
			Class<? extends Value> stratosphereType = StratosphereRelUtils.getTypeClass(firstType);
			for(Record r: result) {
				Value resultVal = r.getField(rowId, stratosphereType);
				Object java =  ( (JavaValue) resultVal).getObjectValue();
				Assert.assertEquals("Returned types to not match", java.getClass(), firstType);
				if(!copy.contains(java)) {
					Assert.fail("Element "+java+" not in expected set");
				} else {
					copy.remove(java);
				}
			}
			Assert.assertEquals("The expected elements were not in the result set", 0, copy.size());
		}
	}
	
	// -- fields of SqlTest
	final private Launcher sqlLauncher = Launcher.getInstance();
	private LocalExecutor stratosphereExecutor = null;
	
	public SqlTest(SqlTestTable tbl) {
		
	}
	/**
	 * Delay start of Stratosphere after SQL parsing.
	 */
	private void ensureStratosphereRunning() {
		if(stratosphereExecutor == null) {
			try {
				stratosphereExecutor = new LocalExecutor();
				stratosphereExecutor.start();
			} catch (Exception e) {
				throw new RuntimeException("Error starting Stratosphere", e);
			}
		}
	}

	public SqlTestResult execute(String sql) {
		Pair<Plan, Collection<Record>> pair = sqlLauncher.convertToPlanWithCollection(sql);
		Plan p = pair.k;
		ensureStratosphereRunning();
		try {
			stratosphereExecutor.executePlan(p);
		} catch (Exception e) {
			throw new RuntimeException("Error executing the plan", e);
		}
		return new SqlTestResult(pair.v);
	}
}
