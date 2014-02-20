package eu.stratosphere.sql.rules;

import net.hydromatic.optiq.rules.java.EnumerableConvention;

import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitSet;

import eu.stratosphere.sql.relOpt.StratosphereSqlProjection;


public class StratosphereProjectionRule extends ConverterRule {

	public static StratosphereProjectionRule INSTANCE = new StratosphereProjectionRule();
	
	/**
	 *  public ConverterRule(
      Class<? extends RelNode> clazz,
      RelTrait in,
      RelTrait out,
      String description) {
	 */
	public StratosphereProjectionRule() {
		//super(RelOptRule.operand(ProjectRel.class, RelOptRule.any()), "StratosphereProjectionRule");
		//super(ProjectRel.class, getInTrait(), getOutTrait(), "StratosphereProjectionRule");
	//	super(RelOptRule.some(ProjectRel.class, Convention.NONE, RelOptRule.any()), "StratosphereProjectionRule");
		 super(ProjectRel.class,
		          Convention.NONE,
		          Convention.NONE,
		          "StratosphereProjectionRule");
	}

/*	@Override
	public void onMatch(RelOptRuleCall call) {
		System.err.println("Here we go");
		final ProjectRel project = (ProjectRel) call.rel(0);
		final RelNode input = call.rel(1);
		final RelTraitSet traits = project.getTraitSet();
		final RelNode convertedInput = convert(input, traits);
		call.transformTo(new StratosphereSqlProjection(project.getCluster(), traits,
				convertedInput, project.getProjects(), project.getRowType(), 0));
	} */

	@Override
	public RelNode convert(RelNode rel) {
		System.err.println("Jaja");
		final ProjectRel project = (ProjectRel) rel;
		final RelTraitSet traits = project.getTraitSet();
		return new StratosphereSqlProjection(project.getCluster(), traits, project.getChild(), project.getProjects(), project.getRowType(), 0);
	}

}
