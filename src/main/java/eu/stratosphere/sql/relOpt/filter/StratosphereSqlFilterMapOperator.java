package eu.stratosphere.sql.relOpt.filter;

import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsExcept;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

@ConstantFieldsExcept({})
public class StratosphereSqlFilterMapOperator extends MapFunction {
	private static final long serialVersionUID = 1L;
	//private Set<StratosphereRexUtils.ProjectionFieldProperties> fields;
	//private String source;
	private Filter filter;

	public StratosphereSqlFilterMapOperator(Filter filter) {
		this.filter = filter;
	}

	@SuppressWarnings({ "deprecation", "unchecked" })
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		// compile gen code
		filter.prepareEvaluation();
	}

	@Override
	public void map(Record record, Collector<Record> out) throws Exception {

		if(filter.evaluate(record)) {
			out.collect(record);
		}
	}

}
