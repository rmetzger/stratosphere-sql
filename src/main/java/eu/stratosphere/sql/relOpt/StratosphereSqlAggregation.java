package eu.stratosphere.sql.relOpt;

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

import eu.stratosphere.api.common.operators.Operator;

public class StratosphereSqlAggregation extends AggregateRelBase implements StratosphereRel {

	
	public StratosphereSqlAggregation(RelOptCluster cluster,
			RelTraitSet traits, RelNode child, BitSet groupSet,
			List<AggregateCall> aggCalls) {
		super(cluster, traits, child, groupSet, aggCalls);
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new StratosphereSqlAggregation(getCluster(), traitSet, sole(inputs), getGroupSet(), getAggCallList());
	}
	

	@Override
	public Operator getStratosphereOperator() {
		throw new RuntimeException("Impl StratOp");
	}

}
