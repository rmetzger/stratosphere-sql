package eu.stratosphere.sql.relOpt.join;

import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

@ConstantFieldsFirstExcept({}) @ConstantFieldsSecondExcept({})
public class StratosphereSqlCrossOperator extends CrossFunction {

	@Override
	public void cross(Record record1, Record record2, Collector<Record> out)
			throws Exception {
		
	}
	
}