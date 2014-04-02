package eu.stratosphere.sql.relOpt;

import java.util.List;

import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.sql.relOpt.filter.Filter;
import eu.stratosphere.sql.relOpt.filter.StratosphereSqlFilterMapOperator;

public class StratosphereSqlFilter	extends FilterRelBase implements StratosphereRel {
	public StratosphereSqlFilter(RelOptCluster cluster, RelTraitSet traits,
			RelNode child, RexNode condition) {
		super(cluster, traits, child, condition);
		Preconditions.checkArgument(getConvention() == CONVENTION);
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		System.err.println("StratosphereSqlFilter.copy()");
		return new StratosphereSqlFilter(getCluster(), traitSet, sole(inputs), getCondition());
	}

	@Override
	public Operator getStratosphereOperator() {
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());
		final RexNode cond = getCondition();

		Filter f = new Filter();
		f.setCondition(cond);
		f.setRexBuilder(getCluster().getRexBuilder());
		f.prepareShipping(getInput(0).getRowType());

        //final RexExecutorImpl executor = new RexExecutorImpl(null);


		Operator filter = MapOperator.builder(new StratosphereSqlFilterMapOperator(f) )
									.input(inputOp)
									.name(condition.toString())
									.build();
		return filter;
	}

}
