package eu.stratosphere.sql;


import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;

public class DataSourceRule extends RelOptRule {

	public static DataSourceRule INSTANCE = new DataSourceRule();
	
	public DataSourceRule() {
		super(RelOptRule.operand(TableAccessRel.class, RelOptRule.any()), "DataSourceRule");
	}
	@Override
	public void onMatch(RelOptRuleCall call) {
		System.err.println("so, da wären wir "+call);
//		final EnumerableTableAccessRel access = (EnumerableTableAccessRel) call.rel(0);
//	    final RelTraitSet traits = access.getTraitSet().plus(DrillRel.CONVENTION);
//	    call.transformTo(new DrillScanRel(access.getCluster(), traits, access.getTable()));
		
	}

}
