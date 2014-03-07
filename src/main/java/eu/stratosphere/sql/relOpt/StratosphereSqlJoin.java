package eu.stratosphere.sql.relOpt;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.sql.StratosphereSQLException;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class StratosphereSqlJoin extends JoinRelBase implements RelNode, StratosphereRel {

	List<Integer> leftKeys;
	List<Integer> rightKeys;
	
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
		Preconditions.checkArgument(joinType == JoinRelType.INNER, "Only inner joins are supported at the moment");
	}

	@Override
	public JoinRelBase copy(RelTraitSet traitSet, RexNode conditionExpr,
			RelNode left, RelNode right, JoinRelType joinType) {
		System.err.println("StratoJoin.copy()");
		return new StratosphereSqlJoin(getCluster(), traitSet, left, 
				right, conditionExpr, joinType, getVariablesStopped());
	}

	@Override
	public Operator getStratosphereOperator() {
		if(getInputs().size() != 2) {
			throw new StratosphereSQLException("The join operator currently supports only join on two inputs");
		}
		leftKeys = new ArrayList<Integer>();
	    rightKeys = new ArrayList<Integer>();
	      RexNode remaining =
	          RelOptUtil.splitJoinCondition(
	              left,
	              right,
	              condition,
	              leftKeys,
	              rightKeys);
	      if (!remaining.isAlwaysTrue()) {
	        throw new StratosphereSQLException(
	            "StratosphereSqlJoinOperator only supports equi-join");
	      }
	      Preconditions.checkArgument( leftKeys.size() == rightKeys.size() );
		RelNode leftRel = getInput(0);
		RelNode rightRel = getInput(1);
		Operator leftOperator = StratosphereRelUtils.toStratoRel(leftRel).getStratosphereOperator();
		Operator rightOperator = StratosphereRelUtils.toStratoRel(rightRel).getStratosphereOperator();
		
		Class<? extends Key>[] types = new Class[leftKeys.size()];
		for(int i = 0; i < leftKeys.size(); i++) {
			
			RelDataType leftRow = leftRel.getExpectedInputRowType(leftKeys.get(i));
			Class<? extends Value> leftType = StratosphereRelUtils.getTypeClass(leftRow);
			
			RelDataType rightRow = leftRel.getExpectedInputRowType(rightKeys.get(i));
			Class<? extends Value> rightType = StratosphereRelUtils.getTypeClass(rightRow);
			// check for equality
			if(!leftType.equals(rightType)) {
				throw new StratosphereSQLException("Types not equal.");
			}
			if(leftType.isAssignableFrom(Key.class)) {
				types[i] = (Class<? extends Key>) leftType;
			} else {
				throw new StratosphereSQLException("Keyfield is not a Key");
			}
		}
		
		
		JoinOperator.Builder joinBuilder = JoinOperator.builder(new StratosphereSqlJoinOperator(), 
				types[0], leftKeys.get(0), rightKeys.get(0));
		for(int i = 1; i < leftKeys.size(); i++) {
			joinBuilder.keyField(types[i], leftKeys.get(0), rightKeys.get(0));
		}
		JoinOperator join = joinBuilder.input1(leftOperator)
				.input2(rightOperator)
				.build();
		return join;
	}

}
