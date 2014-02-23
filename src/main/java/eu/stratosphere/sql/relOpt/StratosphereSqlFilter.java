package eu.stratosphere.sql.relOpt;

import java.util.List;

import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class StratosphereSqlFilter  extends FilterRelBase implements StratosphereRel {
	private RexNode condition;
	public StratosphereSqlFilter(RelOptCluster cluster, RelTraitSet traits,
			RelNode child, RexNode condition) {
		super(cluster, traits, child, condition);
		this.condition = condition;
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		System.err.println("StratosphereSqlFilter.copy()");
		return new StratosphereSqlFilter(getCluster(), getTraitSet(), sole(inputs), getCondition());
	}
	
	public static class StratosphereSqlFilterMapOperator extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	@Override
	public Operator getStratosphereOperator() {
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());
		
		Operator filter = MapOperator.builder(new StratosphereSqlFilterMapOperator() )
									.input(inputOp)
									.name(condition.toString())
									.build();
		return filter;
	}

}
