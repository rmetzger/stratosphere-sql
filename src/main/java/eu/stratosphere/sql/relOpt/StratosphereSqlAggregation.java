package eu.stratosphere.sql.relOpt;

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
import org.eigenbase.sql.fun.SqlAvgAggFunction;
import org.eigenbase.sql.fun.SqlCountAggFunction;
import org.eigenbase.sql.fun.SqlSumAggFunction;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sql.relOpt.aggregate.Aggregates.AbstractAggregate;
import eu.stratosphere.sql.relOpt.aggregate.Aggregates.AvgAggregate;
import eu.stratosphere.sql.relOpt.aggregate.Aggregates.CountAggregate;
import eu.stratosphere.sql.relOpt.aggregate.Aggregates.DecimalAvgAggregate;
import eu.stratosphere.sql.relOpt.aggregate.Aggregates.DecimalSumAggregate;
import eu.stratosphere.sql.relOpt.aggregate.Aggregates.SumAggregate;
import eu.stratosphere.types.DecimalValue;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

public class StratosphereSqlAggregation extends AggregateRelBase implements StratosphereRel {


	public StratosphereSqlAggregation(RelOptCluster cluster,
			RelTraitSet traits, RelNode child, BitSet groupSet,
			List<AggregateCall> aggCalls) {
		super(cluster, traits, child, groupSet, aggCalls);
		System.err.println("Child "+child+" Child rowtype "+child.getRowType());
		Preconditions.checkArgument(getConvention() == CONVENTION);
	}

	@Override
	public AggregateRelBase copy(RelTraitSet traitSet, RelNode input, BitSet groupSet, List<AggregateCall> aggCalls) {
		return new StratosphereSqlAggregation(getCluster(), traitSet, input, groupSet, aggCalls);
	}



	public static class StratosphereSqlAggregationOperator extends ReduceFunction {
		private static final long serialVersionUID = 1L;
		private List<AbstractAggregate> aggFns;
		private int[] groupKeys;
		private Class<? extends Value>[] groupTypes;

		public StratosphereSqlAggregationOperator(List<AbstractAggregate> aggFns, int[] groupKeys, Class<? extends Value>[] groupTypes) {
			this.aggFns = aggFns;
			Preconditions.checkArgument(groupKeys.length == groupTypes.length);
			this.groupKeys = groupKeys;
			this.groupTypes = groupTypes;
		}
		@Override
		public void open(Configuration parameters) throws Exception {
			// TODO Auto-generated method stub
			super.open(parameters);
			this.outRecord = new Record();
		}

		// reduce method fields
		private transient Record outRecord;
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			outRecord.clear();
			boolean first = true;

			for(AbstractAggregate agg : aggFns) {
				agg.initialize();
			}
			while(records.hasNext()) {
				final Record r = records.next();
				if(first) {
					// emit group keys
					for(int i = 0; i < groupKeys.length; i++) {
						outRecord.addField( r.getField(groupKeys[i], groupTypes[i]));
					}
					first = false;
				}
				for(AbstractAggregate agg : aggFns) {
					agg.nextRecord(r);
				}
			}
			for(AbstractAggregate agg : aggFns) {
				Value r = agg.getResult();
				outRecord.addField(r);
			}
			System.err.println("Collecting [aggr] "+outRecord);
			out.collect(outRecord);
		}

	}

	@Override
	public Operator getStratosphereOperator() {
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());

		List<AbstractAggregate> aggFns = new ArrayList<AbstractAggregate>(getAggCallList().size());
		int nr = 0;
		for(AggregateCall call : getAggCallList()) {
			org.eigenbase.rel.Aggregation optiqAgg = call.getAggregation();

			if(optiqAgg instanceof SqlCountAggFunction) {
				CountAggregate count = new CountAggregate();
				aggFns.add(count);
			} else if(optiqAgg instanceof SqlSumAggFunction || optiqAgg instanceof SqlAvgAggFunction) {
				if(call.getArgList().size() != 1) {
					throw new RuntimeException("Implement support for count with mutiple in fields");
				}
				int inFieldPos = call.getArgList().get(0);
				RelDataType colType = getInput(0).getRowType().getFieldList().get(inFieldPos).getType();
				Class<? extends Value> inFieldValueClass = StratosphereRelUtils.getTypeClass(colType);
				RelDataType outColType = call.getType();
				Class<? extends Value> outFieldValueClass = StratosphereRelUtils.getTypeClass(outColType);
				Value inFieldValue;
				Value outFieldValue;
				try {
					inFieldValue = inFieldValueClass.newInstance();
					outFieldValue = outFieldValueClass.newInstance();
				} catch (Exception e) {
					throw new RuntimeException("Error instantiating result Value", e);
				}
				if(optiqAgg instanceof SqlSumAggFunction) {
					if(outFieldValueClass == DecimalValue.class) {
						DecimalSumAggregate sum = new DecimalSumAggregate(inFieldPos);
						aggFns.add(sum);
					} else {
						SumAggregate sum = new SumAggregate(inFieldPos, inFieldValue, outFieldValue);
						aggFns.add(sum);
					}
				} else if(optiqAgg instanceof SqlAvgAggFunction) {
					if(outFieldValueClass == DecimalValue.class) {
						DecimalAvgAggregate decimalAvg = new DecimalAvgAggregate(inFieldPos);
						aggFns.add(decimalAvg);
					} else {
						AvgAggregate avg = new AvgAggregate(inFieldPos, inFieldValue, outFieldValue);
						aggFns.add(avg);
					}
				} else {
					throw new RuntimeException("Thats really not expected");
				}
			//} 
//			else if(optiqAgg instanceof SqlAvgAggFunction) {
//				int inFieldPos = call.getArgList().get(0);
//				RelDataType colType = getInput(0).getRowType().getFieldList().get(inFieldPos).getType();
//				Class<? extends Value> inFieldValueClass = StratosphereRelUtils.getTypeClass(colType);
//				RelDataType outColType = call.getType();
//				Class<? extends Value> outFieldValueClass = StratosphereRelUtils.getTypeClass(outColType);
//				System.err.println("inFieldValueClass"+inFieldValueClass);
			} else {
				throw new RuntimeException("Unsupported aggregation type: "+ optiqAgg);
			}
			nr++;
		}

		Class<? extends Value>[] keyTypes = new Class[getGroupCount()];
		int[] keyIdx = new int[getGroupCount()];
		ReduceOperator.Builder aggBuilder = ReduceOperator.builder(new StratosphereSqlAggregationOperator(aggFns, keyIdx, keyTypes));

		final BitSet groups = getGroupSet();
		int i = 0;
		for(int col = 0; col < getGroupCount(); col++) {
			if(groups.get(col)) {
				RelDataType colType = getInput(0).getRowType().getFieldList().get(col).getType();
				Class<? extends Key<?>> colTypeClass = StratosphereRelUtils.getKeyTypeClass(colType);
				aggBuilder.keyField(colTypeClass, col);
				keyTypes[i] = colTypeClass;
				keyIdx[i] = col;
				i++;
			}
		}
		ReduceOperator aggregation = aggBuilder.build();
		aggregation.setInput(inputOp);

		return aggregation;
	}



}
