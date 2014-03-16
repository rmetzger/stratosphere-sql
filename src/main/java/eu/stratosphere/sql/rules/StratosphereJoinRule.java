package eu.stratosphere.sql.rules;


import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelTraitSet;

import eu.stratosphere.sql.relOpt.StratosphereRel;
import eu.stratosphere.sql.relOpt.StratosphereSqlJoin;

public class StratosphereJoinRule extends ConverterRule {
	final public static StratosphereJoinRule INSTANCE = new StratosphereJoinRule();

	public StratosphereJoinRule() {
		super(JoinRel.class,
					Convention.NONE,
					StratosphereRel.CONVENTION,
					"StratosphereJoinRule");
	}
	@Override
	public RelNode convert(RelNode rel) {
		System.err.println("Join Rule");
		final JoinRel join = (JoinRel) rel;
		
		final RelTraitSet traits = join.getTraitSet().plus(StratosphereRel.CONVENTION);
		
		return new StratosphereSqlJoin( join.getCluster(), traits, 
				convert(join.getLeft(), traits), 
				convert(join.getRight(), traits), 
				join.getCondition(),
				join.getJoinType(), join.getVariablesStopped()
			);
	}
}
