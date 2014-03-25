package eu.stratosphere.sql.relOpt;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.DataContext;

import org.codehaus.janino.JavaSourceClassLoader;
import org.codehaus.janino.util.resource.MapResourceFinder;
import org.codehaus.janino.util.resource.ResourceFinder;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexExecutable;
import org.eigenbase.rex.RexExecutorImpl;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.JavaValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.InstantiationUtil;
import eu.stratosphere.util.ReflectionUtil;

/**
 *
 * It is probably nicer to cleanly recompile the generated code
 * (The current implementation is also recompiling the code, but there is a 
 * lot of other stuff around it (class loaders))
 */
public class StratosphereSqlProjection extends ProjectRelBase implements StratosphereRel {

	//
	// Optiq related
	// 
	public StratosphereSqlProjection(RelOptCluster cluster,
			RelTraitSet traits, RelNode child, List<RexNode> exps,
			RelDataType rowType, int flags) {
		super(cluster, traits, child, exps, rowType, flags);
		Preconditions.checkArgument(getConvention() == CONVENTION);
	}
	
	@Override
	public ProjectRelBase copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps, RelDataType rowType) {
		return new StratosphereSqlProjection(getCluster(), traitSet, input, exps, rowType, getFlags());
	}
	

	//
	// Stratosphere related
	// 
	
	/**
	 * Pass the records through and run Rex against them, if required.
	 */
	public static class StratosphereSqlProjectionMapOperator extends MapFunction {
		private static final long serialVersionUID = 1L;
		private Record outRec = new Record();
		private transient Function1<DataContext, Object[]> function;
		private Set<StratosphereRexUtils.ProjectionFieldProperties> fields;
		Map<String, byte[]> map = new HashMap<String, byte[]>();
		private boolean isTrivial = true; // all fields are projected trivially (no codgen)
		
		public StratosphereSqlProjectionMapOperator(Function1<DataContext, Object[]> function,
				Set<StratosphereRexUtils.ProjectionFieldProperties> fields, String sourceCode) {
			this.function = function;
			this.fields = fields;
			for(StratosphereRexUtils.ProjectionFieldProperties f: fields) {
				if(f.trivialProjection) {
					continue;
				}
				isTrivial = false;
				break;
			}
			if(!isTrivial) {
				String newSrc = "public class "+RexExecutable.GENERATED_CLASS_NAME+" "
						+ "implements net.hydromatic.linq4j.function.Function1, java.io.Serializable { "+sourceCode+" }";
				try {
					map.put(RexExecutable.GENERATED_CLASS_NAME+".java", newSrc.getBytes("UTF-8"));
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException("Error while encoding the generated source", e);
				}
			}
			
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			dataContext = new StratosphereDataContext();
		}
		
		private void writeObject(java.io.ObjectOutputStream stream)
	            throws IOException {
			stream.defaultWriteObject();
			stream.writeObject(function);
	    }

	    private void readObject(java.io.ObjectInputStream stream)
	            throws IOException, ClassNotFoundException {
	    	stream.defaultReadObject();
	    	if(!isTrivial) {
		    	// initialize generated code.
				ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
				ResourceFinder srcFinder = new MapResourceFinder(map);
				JavaSourceClassLoader janinoClassLoader = new JavaSourceClassLoader(currentClassLoader, srcFinder, "UTF-8");
				Thread.currentThread().setContextClassLoader(janinoClassLoader);
				Class<Function1> gen = (Class<Function1>) Class.forName(RexExecutable.GENERATED_CLASS_NAME, true, janinoClassLoader);
		    	function = InstantiationUtil.instantiate(gen, Function1.class);
	    	}
	    }
		
	    // map operator fields
	    private transient Value[] valuesCache;
	    private transient StratosphereDataContext dataContext;
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			outRec.clear();
			if(isTrivial) {
				if(valuesCache == null) {
					valuesCache = new Value[fields.size()];
				}
				for(StratosphereRexUtils.ProjectionFieldProperties field: fields) {
					if(valuesCache[field.fieldIndex] == null) {
						valuesCache[field.fieldIndex] = ReflectionUtil.newInstance(field.inFieldType);
					}
					record.getFieldInto(field.positionInInput, valuesCache[field.fieldIndex]);
					outRec.setField(field.positionInOutput, valuesCache[field.fieldIndex]);
					System.err.println("using field "+field);
				}
				System.err.println("Collecting [trivialProj] "+outRec);
				out.collect(outRec);
			} else {
				Value[] val = new Value[fields.size()];
				for(StratosphereRexUtils.ProjectionFieldProperties field: fields) {
					if(field.trivialProjection) {
						val[field.fieldIndex] = ReflectionUtil.newInstance(field.inFieldType);
						record.getFieldInto(field.positionInInput, val[field.fieldIndex]);
						outRec.setField(field.positionInOutput, val[field.fieldIndex]);
						System.err.println("Copying here since trivial "+field);
						continue;
					}
					if(val[field.fieldIndex] == null) {
						val[field.fieldIndex] = ReflectionUtil.newInstance(field.inFieldType);
					}
					record.getFieldInto(field.positionInInput, val[field.fieldIndex]);
					dataContext.set(field.positionInInput, ((JavaValue) val[field.fieldIndex]).getObjectValue()); // was positionInRex.
				}
				
				// call generated code
		        Object[] result = function.apply(dataContext);
		        for(Object o : result) {
		        	System.err.println("result = "+o);
		        }
		        for(StratosphereRexUtils.ProjectionFieldProperties field: fields) {
		        	if(field.trivialProjection) {
		        		continue;
		        	}
		        	if(field.inFieldType != field.outFieldType) {
		        		val[field.fieldIndex] = ReflectionUtil.newInstance(field.outFieldType);
		        	}
		        	// set result into Value.
		        	((JavaValue) val[field.fieldIndex]).setObjectValue(result[field.positionInRex]);
		        	outRec.setField(field.positionInOutput, val[field.fieldIndex]);
		        	System.err.println("Setting "+val[field.fieldIndex]+" as defined in "+field);
		        }
		        System.err.println("Collecting [complex proj] "+outRec);
				out.collect(outRec);
			}
		}
		
	}
	
	

	@Override
	public Operator getStratosphereOperator() {
		// get Input
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());

		System.err.println("Preparing operator "+this.getDigest());
		final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final RexExecutorImpl executor = new RexExecutorImpl(null);
        final ImmutableList<RexNode> localExps = ImmutableList.copyOf(exps);
        
        StratosphereRexUtils.ReplaceInputRefVisitor replaceInputRefsByExternalInputRefsVisitor = new StratosphereRexUtils.ReplaceInputRefVisitor();
        
        Set<StratosphereRexUtils.ProjectionFieldProperties> fields = new HashSet<StratosphereRexUtils.ProjectionFieldProperties>();
        int pos = 0;
        int rexpos = 0;
        for(RexNode rex : localExps) {
        	rex.accept(replaceInputRefsByExternalInputRefsVisitor);
        	boolean trivialProjection = rex.getKind() == SqlKind.INPUT_REF;
        	for(Pair<Integer, RelDataType> rexInput : replaceInputRefsByExternalInputRefsVisitor.getInputPosAndType() ) {
	        	StratosphereRexUtils.ProjectionFieldProperties field = new StratosphereRexUtils.ProjectionFieldProperties();
	        	field.positionInOutput = pos;
	        	field.fieldIndex = pos;
	        	field.positionInRex = rexpos;
	        	field.positionInInput = rexInput.getKey();
	        	field.inFieldType = StratosphereRelUtils.getTypeClass(rexInput.getValue());
	        	field.outFieldType = StratosphereRelUtils.getTypeClass( getRowType().getFieldList().get(pos).getType() );
	        	field.trivialProjection = trivialProjection;
	        	if(fields.add(field)) {
	        		System.err.println("adding projection field="+field+" for rex="+rex);
	        	} else {
	        		System.err.println("fields already contained "+field+" for rex="+rex);
	        	}
	        	field.name = rex.toString();
        	}
        	pos++;
        	if(!trivialProjection) {
        		rexpos++;
        	}
        	replaceInputRefsByExternalInputRefsVisitor.resetInputList();
        }
      //  if(localExps.size() > 1 && !(localExps.get(0) instanceof RexInputRef)) {
	        // has to be called after ReplaceInputRefVisitor shuttle went over tree to ensure "external" flag on InputRef
	        RexExecutable executable = executor.createExecutable(rexBuilder, localExps);
      //  }
        
		// create MapOperator
		MapOperator proj = MapOperator	.builder(new StratosphereSqlProjectionMapOperator(executable.getFunction(),
																						fields, executable.getSource()))
										.input(inputOp)
										.name(buildName())
										.build();
		return proj;
	}
	
	  private String buildName() {
		return "Project "+getRowType().toString();
	}

	

	public Class<? extends Value>[] getFields() {
		Class<? extends Value>[] fields = new Class[this.exps.size()];
		Iterator<RexNode> it = exps.iterator();
		int i = 0;
		while(it.hasNext()) {
			RexInputRef inputRef = (RexInputRef) it.next();
			fields[i++] = StratosphereRelUtils.getTypeClass(inputRef.getType());
		}
		return fields;
	}

	
}
