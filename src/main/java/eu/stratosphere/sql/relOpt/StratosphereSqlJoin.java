package eu.stratosphere.sql.relOpt;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.operators.CrossWithLargeOperator;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.sql.StratosphereSQLException;
import eu.stratosphere.sql.relOpt.filter.Filter;
import eu.stratosphere.sql.relOpt.join.StratosphereSqlCrossOperator;
import eu.stratosphere.sql.relOpt.join.StratosphereSqlJoinOperator;
import eu.stratosphere.types.Key;

public class StratosphereSqlJoin extends JoinRelBase implements RelNode, StratosphereRel {
	private static final Log LOG = LogFactory.getLog(StratosphereSqlJoin.class);

	private List<Integer> leftKeys;
	private List<Integer> rightKeys;

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
		Preconditions.checkArgument( leftKeys.size() == rightKeys.size() );
		RelNode leftRel = getInput(0);
		RelNode rightRel = getInput(1);
		Operator leftOperator = StratosphereRelUtils.toStratoRel(leftRel).getStratosphereOperator();
		Operator rightOperator = StratosphereRelUtils.toStratoRel(rightRel).getStratosphereOperator();

		Class<? extends Key<?>>[] types = new Class[leftKeys.size()];
		for(int i = 0; i < leftKeys.size(); i++) {

			//leftRel.getExpectedInputRowType(leftKeys.get(i));
			RelDataType leftRow = leftRel.getRowType().getFieldList().get(leftKeys.get(i)).getType();
			Class<? extends Key<?>> leftType = StratosphereRelUtils.getKeyTypeClass(leftRow);

			RelDataType rightRow = rightRel.getRowType().getFieldList().get(rightKeys.get(i)).getType();
			Class<? extends Key<?>> rightType = StratosphereRelUtils.getKeyTypeClass(rightRow);
			// check for equality
			if(!leftType.equals(rightType)) {
				throw new StratosphereSQLException("Types not equal.");
			}
			types[i] = leftType;
		}
		if (!remaining.isAlwaysTrue() && leftKeys.size() == 0) {
			// cartesian product
			LOG.warn("Join "+this+" is executing a cartesian product");
			Filter f = new Filter();
			f.setCondition(remaining);
			f.setRexBuilder(getCluster().getRexBuilder());
			f.prepareShipping(getRowType());
			CrossWithLargeOperator.Builder crossBuilder = CrossWithLargeOperator.builder(new StratosphereSqlCrossOperator(f) );
			double leftEst = RelMetadataQuery.getRowCount(leftRel);
			double rightEst = RelMetadataQuery.getRowCount(rightRel);
			LOG.info("LeftEst="+leftEst+" rightEst="+rightEst);
			if(leftEst >= rightEst) {
				crossBuilder.input1(leftOperator);
				crossBuilder.input2(rightOperator);
			} else {
				crossBuilder.input1(rightOperator);
				crossBuilder.input2(leftOperator);
			}
			return crossBuilder.build();
		}


		JoinOperator.Builder joinBuilder = JoinOperator.builder(new StratosphereSqlJoinOperator(),
				types[0], leftKeys.get(0), rightKeys.get(0));
		for(int i = 1; i < leftKeys.size(); i++) {
			joinBuilder.keyField(types[i], leftKeys.get(i), rightKeys.get(i));
		}
		JoinOperator join = joinBuilder.input1(leftOperator)
				.input2(rightOperator)
				.build();
		return join;
	}

}
