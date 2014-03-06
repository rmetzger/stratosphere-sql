package eu.stratosphere.sql.relOpt;

import java.util.Set;

import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class StratosphereSqlJoin extends JoinRelBase implements RelNode, StratosphereRel {

	public static class StratosphereSqlJoinOperator extends JoinFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void join(Record value1, Record value2, Collector<Record> out)
				throws Exception {
			//
		}
		
	}
	public StratosphereSqlJoin(RelOptCluster cluster, RelTraitSet traits,
			RelNode left, RelNode right, RexNode condition,
			JoinRelType joinType, Set<String> variablesStopped) {
		super(cluster, traits, left, right, condition, joinType, variablesStopped);
	}

	@Override
	public JoinRelBase copy(RelTraitSet traitSet, RexNode conditionExpr,
			RelNode left, RelNode right, JoinRelType joinType) {
		System.err.println("StratoJoin.copy()");
		return new StratosphereSqlJoin(getCluster(), getTraitSet(), getLeft(), 
				getRight(), getCondition(), getJoinType(), getVariablesStopped());
	}

	@Override
	public Operator getStratosphereOperator() {
		
		Operator leftOperator = null;
		Operator rightOperator = null;
		JoinOperator join = JoinOperator.builder(new StratosphereSqlJoinOperator(), 
				StringValue.class, 0, 0)
				.input1(leftOperator)
				.input2(rightOperator)
				.build();
		return join;
	}

}
