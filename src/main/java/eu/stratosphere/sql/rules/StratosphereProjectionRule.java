package eu.stratosphere.sql.rules;

import org.eigenbase.rel.ProjectRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;


public class StratosphereProjectionRule extends RelOptRule {

	public static StratosphereProjectionRule INSTANCE = new StratosphereProjectionRule();
	
	public StratosphereProjectionRule() {
		super(RelOptRule.operand(ProjectRel.class, RelOptRule.any()), "StratosphereProjectionRule");
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		System.err.println("Here we go");
	}

}
