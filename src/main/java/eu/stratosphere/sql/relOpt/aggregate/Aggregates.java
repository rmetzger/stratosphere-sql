package eu.stratosphere.sql.relOpt.aggregate;

import java.io.Serializable;
import java.math.BigDecimal;

import eu.stratosphere.types.DecimalValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.JavaValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;

public class Aggregates {
	public static abstract class AbstractAggregate implements Serializable {
		public abstract void initialize();
		public abstract void nextRecord(Record r);
		public abstract Value getResult();
	}

	public static class AvgAggregate extends SumAggregate {
		private long count;

		public AvgAggregate(int inFieldPos, Value inFieldValue, Value outFieldValue) {
			super(inFieldPos, inFieldValue, outFieldValue);
		}

		@Override
		public void initialize() {
			super.initialize();
			count = 0;
		}

		@Override
		public void nextRecord(Record r) {
			super.nextRecord(r);
			count++;
		}

		@Override
		public  Value getResult() {
			long result = this.sum/this.count;
			return returnFromLong(result);
		}

	}

	public static class CountAggregate extends AbstractAggregate {
		private long count = 0;
		private LongValue result = new LongValue();
		@Override
		public void initialize() {
			count = 0;
		}

		@Override
		public void nextRecord(Record r) {
			count++;
		}

		@Override
		public Value getResult() {
			result.setValue(count);
			return result;
		}
	}
	
	public static class DecimalAvgAggregate extends DecimalSumAggregate {
		private long count;
		
		public DecimalAvgAggregate(int inFieldPos) {
			super(inFieldPos);
		}

		@Override
		public void initialize() {
			super.initialize();
			count = 0;
		}

		@Override
		public void nextRecord(Record r) {
			super.nextRecord(r);
			count++;
		}

		@Override
		public Value getResult() {
			DecimalValue r = new DecimalValue();
			r.setValue(value.divide(new BigDecimal(count)));
			return r;
		}
		
	}

	public static class DecimalSumAggregate extends AbstractAggregate {
		private int inFieldPos;
		protected BigDecimal value;
		public DecimalSumAggregate(int inFieldPos) {
			this.inFieldPos = inFieldPos;
		}

		@Override
		public void initialize() {
			value = new BigDecimal(0);
		}

		@Override
		public void nextRecord(Record r) {
			BigDecimal v = r.getField(inFieldPos, DecimalValue.class).getValue();
			value.add(v);
		}

		@Override
		public Value getResult() {
			DecimalValue r = new DecimalValue();
			r.setValue(value);
			return r;
		}
	}

	public static class SumAggregate extends AbstractAggregate {
		protected long sum;
		private int inFieldPos = 0;
		private Value inFieldValue;
		private Value outFieldValue;

		public SumAggregate(int inFieldPos, Value inFieldValue, Value outFieldValue) {
			this.inFieldPos = inFieldPos;
			this.inFieldValue = inFieldValue;
			this.outFieldValue = outFieldValue;
		}

		@Override
		public void initialize() {
			sum = 0L;
		}
		
		@Override
		public void nextRecord(Record r) {
			r.getFieldInto(inFieldPos, inFieldValue);
			// todo might also be int?
			sum +=  ( (Number) ( (JavaValue) inFieldValue).getObjectValue() ).longValue();
		}
		@Override
		public Value getResult() {
			return returnFromLong(sum);
		}
		protected Value returnFromLong(long l) {
			if(outFieldValue instanceof LongValue) {
				( (JavaValue)outFieldValue).setObjectValue(l);
				return outFieldValue;
			} else if(outFieldValue instanceof IntValue) {
				( (JavaValue)outFieldValue).setObjectValue( (int) l );
				return outFieldValue;
			} else {
				throw new RuntimeException("Unsupported sum aggregate field type: "+outFieldValue.getClass().getName());
			}
		}
	}
}
