package eu.stratosphere.sql.relOpt;

import java.util.Set;

import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.sql.StratosphereSQLRuntimeException;
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
		Preconditions.checkArgument(getConvention() == CONVENTION);
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
		if(getInputs().size() != 2) {
			throw new StratosphereSQLRuntimeException("The join operator currently supports only join on two inputs");
		}
		RelNode leftRel = getInput(0);
		RelNode rightRel = getInput(1);
		Operator leftOperator = StratosphereRelUtils.toStratoRel(leftRel).getStratosphereOperator();
		Operator rightOperator = StratosphereRelUtils.toStratoRel(rightRel).getStratosphereOperator();
		JoinOperator join = JoinOperator.builder(new StratosphereSqlJoinOperator(), 
				StringValue.class, 0, 0)
				.input1(leftOperator)
				.input2(rightOperator)
				.build();
		return join;
	}

}
