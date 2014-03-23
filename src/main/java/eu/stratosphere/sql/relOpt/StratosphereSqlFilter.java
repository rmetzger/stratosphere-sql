package eu.stratosphere.sql.relOpt;

import java.io.Serializable;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.runtime.Utilities;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexExecutable;
import org.eigenbase.rex.RexExecutorImpl;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsExcept;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.sql.relOpt.StratosphereRexUtils.ReplaceInputRefVisitor;
import eu.stratosphere.types.JavaValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.ReflectionUtil;

public class StratosphereSqlFilter	extends FilterRelBase implements StratosphereRel {
	public StratosphereSqlFilter(RelOptCluster cluster, RelTraitSet traits,
			RelNode child, RexNode condition) {
		super(cluster, traits, child, condition);
		Preconditions.checkArgument(getConvention() == CONVENTION);
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		System.err.println("StratosphereSqlFilter.copy()");
		return new StratosphereSqlFilter(getCluster(), traitSet, sole(inputs), getCondition());
	}
	
	@ConstantFieldsExcept({})
	public static class StratosphereSqlFilterMapOperator extends MapFunction {
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
	
	@Override
	public Operator getStratosphereOperator() {
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());
		RexNode cond = getCondition();
		
		final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final RexExecutorImpl executor = new RexExecutorImpl(null);
        StratosphereRexUtils.ReplaceInputRefVisitor replaceInputRefsByExternalInputRefsVisitor = new StratosphereRexUtils.ReplaceInputRefVisitor();
        cond.accept(replaceInputRefsByExternalInputRefsVisitor);
        
        final ImmutableList<RexNode> localExps = ImmutableList.of(cond);
		
		
        Set<StratosphereRexUtils.ProjectionFieldProperties> fields = new HashSet<StratosphereRexUtils.ProjectionFieldProperties>();
        int pos = 0;
    	for(Pair<Integer, RelDataType> rexInput : replaceInputRefsByExternalInputRefsVisitor.getInputPosAndType() ) {
        	StratosphereRexUtils.ProjectionFieldProperties field = new StratosphereRexUtils.ProjectionFieldProperties();
        	field.fieldIndex = pos++;
        	field.positionInInput = rexInput.getKey();
        	field.inFieldType = StratosphereRelUtils.getTypeClass(rexInput.getValue());
        	field.name = cond.toString();
        	fields.add(field);
    	}
        RexExecutable executable = executor.createExecutable(rexBuilder, localExps);
		System.err.println("Code: "+executable.getSource());
        
		Operator filter = MapOperator.builder(new StratosphereSqlFilterMapOperator(executable.getSource(), fields) )
									.input(inputOp)
									.name(condition.toString())
									.build();
		return filter;
	}

}
