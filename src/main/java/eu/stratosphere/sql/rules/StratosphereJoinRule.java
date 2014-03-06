package eu.stratosphere.sql.rules;

import java.util.Set;

import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import eu.stratosphere.sql.relOpt.StratosphereRel;
import eu.stratosphere.sql.relOpt.StratosphereSqlJoin;

public class StratosphereJoinRule extends ConverterRule {
	public static StratosphereJoinRule INSTANCE = new StratosphereJoinRule();

	public StratosphereJoinRule() {
		super(JoinRel.class,
		          Convention.NONE,
		          Convention.NONE,
		          "StratosphereJoinRule");
	}
	@Override
	public RelNode convert(RelNode rel) {
		System.err.println("Join Rule");
		final JoinRel join = (JoinRel) rel;
		
		final RelTraitSet traits = join.getTraitSet().plus(StratosphereRel.CONVENTION);
		
		//  RelOptCluster cluster, RelTraitSet traits,
		//  RelNode left, RelNode right, RexNode condition,
		//  JoinRelType joinType, Set<String> variablesStopped
		return new StratosphereSqlJoin(
				join.getCluster(), traits, 
				convert(join.getLeft(), traits), convert(join.getRight(), traits), join.getCondition(),
				join.getJoinType(), join.getVariablesStopped()
				);
	}
}
