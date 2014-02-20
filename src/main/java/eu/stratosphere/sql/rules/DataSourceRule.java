package eu.stratosphere.sql.rules;


import net.hydromatic.optiq.rules.java.JavaRules.EnumerableTableAccessRel;

import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

import eu.stratosphere.sql.relOpt.StratosphereDataSource;

public class DataSourceRule extends RelOptRule {

	public static DataSourceRule INSTANCE = new DataSourceRule();
	
	public DataSourceRule() {
		super(RelOptRule.operand(TableAccessRel.class, RelOptRule.any()), "DataSourceRule");
	}
	
	@Override
	public void onMatch(RelOptRuleCall call) {
		System.err.println("so, da wären wir "+call);
		final TableAccessRel tbl = call.rel(0);
		// RelOptCluster cluster, RelTraitSet traits, RelOptTable table
		call.transformTo(new StratosphereDataSource(tbl.getCluster(), tbl.getTraitSet(),));
//		final EnumerableTableAccessRel access = (EnumerableTableAccessRel) call.rel(0);
//	    final RelTraitSet traits = access.getTraitSet().plus(DrillRel.CONVENTION);
//	    call.transformTo(new DrillScanRel(access.getCluster(), traits, access.getTable()));
		
	}

}
