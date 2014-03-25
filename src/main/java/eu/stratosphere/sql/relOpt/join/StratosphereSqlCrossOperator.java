package eu.stratosphere.sql.relOpt.join;

import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sql.relOpt.filter.Filter;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

@ConstantFieldsFirstExcept({}) @ConstantFieldsSecondExcept({})
public class StratosphereSqlCrossOperator extends CrossFunction {
	private static final long serialVersionUID = 1L;
	
	private Filter filter;
	public StratosphereSqlCrossOperator(Filter f) {
		this.filter = f;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		// TODO Auto-generated method stub
		super.open(parameters);
		filter.prepareEvaluation();
	}
	@Override
	public void cross(Record record1, Record record2, Collector<Record> out)
			throws Exception {
		if(filter.evaluateTwo(record1, record2)) {
			System.err.println("Cross got rec1="+record1+" rec2="+record2);
			record1.concatenate(record2);
			System.err.println("Cross is outputting "+record1);
			out.collect(record1);
			
		}
	}
	
}