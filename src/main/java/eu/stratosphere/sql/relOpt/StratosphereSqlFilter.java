package eu.stratosphere.sql.relOpt;

import java.util.List;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
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
		private static final JexlEngine jexl = new JexlEngine();
        static {
           jexl.setCache(512);
           jexl.setLenient(false);
           jexl.setSilent(false);
        }
        private transient RexNode condition; // testing only
        
		public StratosphereSqlFilterMapOperator(RexNode condition) {
			this.condition = condition;
		}

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			// TODO Auto-generated method stub
			// Expression e = jexl.createExpression(expression)
		}
		
		// testing only
		public RexNode getCondition() {
			return this.condition;
		}
	}
	
	@Override
	public Operator getStratosphereOperator() {
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());
		
		Operator filter = MapOperator.builder(new StratosphereSqlFilterMapOperator(getCondition()) )
									.input(inputOp)
									.name(condition.toString())
									.build();
		return filter;
	}

}
