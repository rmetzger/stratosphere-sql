package eu.stratosphere.sql.relOpt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.fun.SqlCountAggFunction;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.sql.relOpt.StratosphereSqlAggregation.Aggregation.Type;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class StratosphereSqlAggregation extends AggregateRelBase implements StratosphereRel {

	
	public StratosphereSqlAggregation(RelOptCluster cluster,
			RelTraitSet traits, RelNode child, BitSet groupSet,
			List<AggregateCall> aggCalls) {
		super(cluster, traits, child, groupSet, aggCalls);
		Preconditions.checkArgument(getConvention() == CONVENTION);
	}

	@Override
	public AggregateRelBase copy(RelTraitSet traitSet, RelNode input, BitSet groupSet, List<AggregateCall> aggCalls) {
		return new StratosphereSqlAggregation(getCluster(), traitSet, input, groupSet, aggCalls);
	}

	
	public static class StratosphereSqlAggregationOperator extends ReduceFunction {

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public static class Aggregation implements Serializable {
		private static final long serialVersionUID = 1L;
		
		public static enum Type { COUNT, SUM }
		
		public Type type;
		public Class<? extends Value> inputType;
		public boolean isDistinct;
		public int inputPos;
	}
	@Override
	public Operator getStratosphereOperator() {
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());
		
		List<Aggregation> aggFns = new ArrayList<Aggregation>(getAggCallList().size());
		
		for(AggregateCall call : getAggCallList()) {
			Aggregation agg = new Aggregation();
			org.eigenbase.rel.Aggregation optiqAgg = call.getAggregation();
			if(optiqAgg instanceof SqlCountAggFunction) {
				agg.type = Type.COUNT;
			}
			agg.isDistinct = call.isDistinct();
			
			aggFns.add(agg);
		}
		
		ReduceOperator.Builder aggBuilder = ReduceOperator.builder(new StratosphereSqlAggregationOperator());
		
		final BitSet groups = getGroupSet();
		for(int col = 0; col < getGroupCount(); col++) {
			if(groups.get(col)) {
				RelDataType colType = getInput(0).getRowType().getFieldList().get(col).getType();
				aggBuilder.keyField(StratosphereRelUtils.getKeyTypeClass(colType), col);
			}
		}
		ReduceOperator aggregation = aggBuilder.build();
		aggregation.setInput(inputOp);
		
		return aggregation;
	}

	

}
