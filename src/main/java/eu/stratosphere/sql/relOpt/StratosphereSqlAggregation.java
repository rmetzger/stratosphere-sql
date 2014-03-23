package eu.stratosphere.sql.relOpt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import javax.sound.midi.SysexMessage;

import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.fun.SqlCountAggFunction;
import org.eigenbase.sql.fun.SqlSumAggFunction;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sql.relOpt.StratosphereSqlAggregation.Aggregation.Type;
import eu.stratosphere.types.JavaValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.InstantiationUtil;

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

	public static abstract class AbstractAggregate implements Serializable {
		abstract void initialize();
		abstract void nextRecord(Record r);
		abstract Value getResult();
	}
	public static class CountAggregate extends AbstractAggregate {
		private long count = 0;
		private LongValue result = new LongValue();
		@Override
		void initialize() {
			count = 0;
		}

		@Override
		void nextRecord(Record r) {
			count++;
		}

		@Override
		Value getResult() {
			result.setValue(count);
			return result;
		}
	}
	public static class SumAggregate extends AbstractAggregate {
		private long sum = 0;
		private LongValue result = new LongValue();
		private int inFieldPos = 0;
		private Value inFieldValue;
		
		public SumAggregate(int inFieldPos, Value inFieldValue) {
			this.inFieldPos = inFieldPos;
			this.inFieldValue = inFieldValue;
		}
		
		@Override
		void initialize() {
			sum = 0;
		}
		@Override
		void nextRecord(Record r) {
			r.getFieldInto(inFieldPos, inFieldValue);
			// todo might also be int?
			sum += (Integer) ( (JavaValue) inFieldValue).getObjectValue();
		}
		@Override
		Value getResult() {
			result.setValue(sum);
			return result;
		}
	}
	
	public static class StratosphereSqlAggregationOperator extends ReduceFunction {
		private static final long serialVersionUID = 1L;
		private List<AbstractAggregate> aggFns;
		
		public StratosphereSqlAggregationOperator(List<AbstractAggregate> aggFns) {
			this.aggFns = aggFns;
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
			for(AbstractAggregate agg : aggFns) {
				agg.initialize();
			}
			while(records.hasNext()) {
				final Record r = records.next();
				for(AbstractAggregate agg : aggFns) {
					agg.nextRecord(r);
				}
			}
			for(AbstractAggregate agg : aggFns) {
				Value r = agg.getResult();
				outRecord.addField(r);
			}
			System.err.println("Collecting "+outRecord);
			out.collect(outRecord);
//			if(outputCache == null) {
//				outputCache = new Value[aggFns.size()];
//			}
//			if(inputCache == null) {
//				inputCache = new Value[aggFns.size()];
//			}
//			int i = 0;
//			while(records.hasNext()) {
//			for(Aggregation aggr : aggFns) {
//				if(outputCache[i] == null) {
//					outputCache[i] = aggr.outputType.newInstance();
//				}
//				switch(aggr.type) {
//					case COUNT:
//						long cnt = 0;
//						
//							records.next();
//							cnt++;
//						}
//						((JavaValue) outputCache[i]).setObjectValue(cnt);
//						break;
//					case SUM:
//						long sum = 0;
//						while(records.hasNext()) {
//							Record r = records.next();
//							r.get
//						}
//						((JavaValue) valuesCache[i]).setObjectValue(sum);
//						break;
//					default:
//						throw new RuntimeException("Unknown aggregation type "+aggr.type);
//				}
//				i++;
//			}
		}
		
	}
	
	public static class Aggregation implements Serializable {
		private static final long serialVersionUID = 1L;
		
		public static enum Type { COUNT, SUM }
		
		public Type type;
		public Class<? extends Value> inputType;
		public Class<? extends Value> outputType;
		public boolean isDistinct;
		public List<Integer> inputPositions;
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
			} else if(optiqAgg instanceof SqlSumAggFunction) {
				if(call.getArgList().size() != 1) {
					throw new RuntimeException("Implement support for count with mutiple in fields");
				}
				int inFieldPos = call.getArgList().get(0);
				RelDataType colType = getInput(0).getRowType().getFieldList().get(inFieldPos).getType();
				Class<? extends Value> inFieldValueClass = StratosphereRelUtils.getTypeClass(colType);
				Value inFieldValue;
				try {
					inFieldValue = inFieldValueClass.newInstance();
				} catch (Exception e) {
					throw new RuntimeException("Error instantiating result Value", e);
				}
				SumAggregate sum = new SumAggregate(inFieldPos, inFieldValue);
				aggFns.add(sum);
			} else {
				throw new RuntimeException("Unsupported aggregation type: "+ optiqAgg);
			}
			nr++;
//			Aggregation agg = new Aggregation();
//			org.eigenbase.rel.Aggregation optiqAgg = call.getAggregation();
//			if(optiqAgg instanceof SqlCountAggFunction) {
//				agg.type = Type.COUNT;
//			}
//			agg.isDistinct = call.isDistinct();
//			agg.inputPositions = call.getArgList();
			
			//aggFns.add(agg);
		}
		
		ReduceOperator.Builder aggBuilder = ReduceOperator.builder(new StratosphereSqlAggregationOperator(aggFns));
		
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
