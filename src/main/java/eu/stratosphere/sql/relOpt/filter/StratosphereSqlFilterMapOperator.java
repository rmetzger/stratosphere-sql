package eu.stratosphere.sql.relOpt.filter;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Set;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.runtime.Utilities;

import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;
import org.eigenbase.rex.RexExecutable;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsExcept;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sql.relOpt.StratosphereDataContext;
import eu.stratosphere.sql.relOpt.StratosphereRexUtils;
import eu.stratosphere.sql.relOpt.StratosphereRexUtils.ProjectionFieldProperties;
import eu.stratosphere.types.JavaValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.ReflectionUtil;

@ConstantFieldsExcept({})
public class StratosphereSqlFilterMapOperator extends MapFunction {
	private static final long serialVersionUID = 1L;
	private Set<StratosphereRexUtils.ProjectionFieldProperties> fields;
	private String source;
	private transient Function1<DataContext, Object[]> function;
	
	public StratosphereSqlFilterMapOperator(String source, Set<StratosphereRexUtils.ProjectionFieldProperties> fields) {
		this.source = source;
		this.fields = fields;
	}
	@SuppressWarnings({ "deprecation", "unchecked" })
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		// compile gen code
		this.function =  (Function1<DataContext, Object[]>) ClassBodyEvaluator.createFastClassBodyEvaluator(
                  new Scanner(null, new StringReader(source)),
                  RexExecutable.GENERATED_CLASS_NAME,
                  Utilities.class,
                  new Class[]{Function1.class , Serializable.class},
                  getClass().getClassLoader());
	}

	 // map operator fields
    private transient Value[] valuesCache;
    private transient StratosphereDataContext dataContext;
    
    
	@Override 
	public void map(Record record, Collector<Record> out) throws Exception {
		if(this.dataContext == null) {
			dataContext = new StratosphereDataContext();
		}
		if(valuesCache == null) {
			valuesCache = new Value[fields.size()];
		}
		// prepare variables
		for(StratosphereRexUtils.ProjectionFieldProperties field: fields) {
			if(valuesCache[field.fieldIndex] == null) {
				valuesCache[field.fieldIndex] = ReflectionUtil.newInstance(field.inFieldType);
			}
			record.getFieldInto(field.positionInInput, valuesCache[field.fieldIndex]);
			dataContext.set(field.positionInInput, ((JavaValue) valuesCache[field.fieldIndex]).getObjectValue());
		}
		Object[] result = function.apply(dataContext);
        for(Object o : result) {
        	System.err.println("result = "+o);
        }
        if((Boolean) result[0]) {
        	out.collect(record);
        }
	}
	
}