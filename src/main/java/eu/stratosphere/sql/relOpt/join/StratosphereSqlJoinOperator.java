package eu.stratosphere.sql.relOpt.join;

import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

@ConstantFieldsFirstExcept({}) @ConstantFieldsSecondExcept({})
public class StratosphereSqlJoinOperator extends JoinFunction {
	private static final long serialVersionUID = 1L;

	@Override
	public void join(Record value1, Record value2, Collector<Record> out)
			throws Exception {
		value1.concatenate(value2);
		System.err.println("Join is emitting "+value1);
		out.collect(value1);
	}
	
}