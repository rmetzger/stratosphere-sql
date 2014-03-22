package eu.stratosphere.sql.relOpt;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.Schemas;

import org.apache.commons.lang3.builder.HashCodeBuilder;
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
import org.eigenbase.rex.RexVisitor;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.util.FieldAnnotations.SerializableField;
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
 *
 *			TODO: Add short cut for trivial projection.
 *
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
	
	public static class ProjectionFieldProperties implements Serializable {
		private static final long serialVersionUID = 1L;
		public Class<? extends Value> fieldType;
		public int positionInInput;
		public int positionInOutput;
		public int positionInRex;
		public int fieldIndex;
		
		@Override
		public String toString() {
			return "[ProjectionFieldProperties: fieldType="+fieldType+", "
					+ "positionInInput="+positionInInput+", "
					+ "positionInOutput="+positionInOutput+", "
					+ "positionInRex="+positionInRex+", "
					+ "fieldIndex="+fieldIndex+"]";
		}
		@Override
		public int hashCode() {
			HashCodeBuilder hash = new HashCodeBuilder();
			hash.append(positionInInput);
			hash.append(positionInOutput);
			hash.append(positionInRex);
			hash.append(fieldIndex);
			return hash.toHashCode();
		}
		
		
		public boolean equals(Object obj) {
			if(obj instanceof ProjectionFieldProperties) {
				ProjectionFieldProperties other = (ProjectionFieldProperties) obj;
				return other.positionInInput == positionInInput &&
						other.positionInOutput == positionInOutput &&
						other.positionInRex == positionInRex &&
						other.fieldIndex == fieldIndex;
			}
			return false;
		}
	}
	
	/**
	 * Pass the records through and run Rex against them, if required.
	 */
	public static class StratosphereSqlProjectionMapOperator extends MapFunction {
		private static final long serialVersionUID = 1L;
		private Record outRec = new Record();
		private transient Function1<DataContext, Object[]> function;
		private Set<ProjectionFieldProperties> fields;
		Map<String, byte[]> map = new HashMap<String, byte[]>();
		
		public StratosphereSqlProjectionMapOperator(Function1<DataContext, Object[]> function,
				Set<ProjectionFieldProperties> fields, String sourceCode) {
			this.function = function;
			this.fields = fields;
			String newSrc = "public class "+RexExecutable.GENERATED_CLASS_NAME+" "
					+ "implements net.hydromatic.linq4j.function.Function1, java.io.Serializable { "+sourceCode+" }";
			try {
				map.put(RexExecutable.GENERATED_CLASS_NAME+".java", newSrc.getBytes("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Error while encoding the generated source", e);
			}
			
		}
		
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			
		}
		
		private void writeObject(java.io.ObjectOutputStream stream)
	            throws IOException {
			stream.defaultWriteObject();
			stream.writeObject(function);
	    }

	    private void readObject(java.io.ObjectInputStream stream)
	            throws IOException, ClassNotFoundException {
	    	stream.defaultReadObject();
	    	
	    	// initialize generated code.
			ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
			ResourceFinder srcFinder = new MapResourceFinder(map);
			JavaSourceClassLoader janinoClassLoader = new JavaSourceClassLoader(currentClassLoader, srcFinder, "UTF-8");
			Thread.currentThread().setContextClassLoader(janinoClassLoader);
			Class<Function1> gen = (Class<Function1>) Class.forName(RexExecutable.GENERATED_CLASS_NAME, true, janinoClassLoader);
	    	function = InstantiationUtil.instantiate(gen, Function1.class);
	    }
		

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			outRec.clear();
			Map<String, Object> map = new HashMap();
			DataContext dataContext = new FakeItDataContext(map);
			int i = 0;
			Value[] val = new Value[fields.size()];
			for(ProjectionFieldProperties field: fields) {
				field.fieldIndex = i;
				if(val[i] == null) {
					val[i] = ReflectionUtil.newInstance(field.fieldType);
				}
				record.getFieldInto(field.positionInInput, val[i]);
				map.put("?"+field.positionInInput, ((JavaValue) val[i]).getObjectValue()); // was positionInRex.
				i++;
			}
			
			// call generated code
	        Object[] result = function.apply(dataContext);
	        for(Object o : result) {
	        	System.err.println("result = "+o);
	        }
	        for(ProjectionFieldProperties field: fields) {
	        	// set result into Value.
	        	((JavaValue) val[field.fieldIndex]).setObjectValue(result[field.fieldIndex]);
	        	outRec.setField(field.positionInOutput, val[field.fieldIndex]);
	        }
	        System.err.println("Collecting "+outRec);
			out.collect(outRec);
		}
		
	}
	
	

	@Override
	public Operator getStratosphereOperator() {
		// get Input
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());

		final RexBuilder rexBuilder = getCluster().getRexBuilder();
        
        final RexExecutorImpl executor = new RexExecutorImpl();
        
        ImmutableList<RexNode> localExps = ImmutableList.copyOf(exps);
        
        ReplaceInputRefVisitor replaceInputRefsByExternalInputRefsVisitor = new ReplaceInputRefVisitor();
        
        
        Set<ProjectionFieldProperties> fields = new HashSet<ProjectionFieldProperties>();
        int pos = 0;
        for(RexNode rex : localExps) {
        	rex.accept(replaceInputRefsByExternalInputRefsVisitor);
        	
        	for(Pair<Integer, RelDataType> rexInput : replaceInputRefsByExternalInputRefsVisitor.getInputPosAndType() ) {
	        	ProjectionFieldProperties field = new ProjectionFieldProperties();
	        	field.positionInOutput = pos;
	        	field.fieldIndex = pos;
	        	field.positionInInput = rexInput.getKey();
	        	field.fieldType = StratosphereRelUtils.getTypeClass(rexInput.getValue());
	        	fields.add(field);
	        	System.err.println("adding projection field "+field);
        	}
        	pos++;
        	replaceInputRefsByExternalInputRefsVisitor.resetInputList();
        }
        // has to be called after ReplaceInputRefVisitor shuttle went over tree to ensure "external" flag on InputRef
        RexExecutable executable = executor.createExecutable(rexBuilder, localExps);
        
		// create MapOperator
		MapOperator proj = MapOperator	.builder(new StratosphereSqlProjectionMapOperator(executable.getFunction(),
																						fields, executable.getSource()))
										.input(inputOp)
										.name(buildName())
										.build();
		return proj;
	}
	
	  private class ReplaceInputRefVisitor extends RexVisitorImpl<Void> {
		private List<Pair<Integer, RelDataType>> inputPosAndType = new ArrayList<Pair<Integer, RelDataType>>();
	    public ReplaceInputRefVisitor() {
	      super(true);
	      
	    }

	    public Void visitInputRef(RexInputRef inputRef) {
	      if(true) { // do I need a condition here?
	    	  inputRef.setExternalRef(true);
	      }
	      System.err.println("Setting input ref "+inputRef+" index "+inputRef.getIndex());
	      inputPosAndType.add(Pair.of(inputRef.getIndex(), inputRef.getType()));
	      return null;
	    }
	    public void resetInputList() {
	    	inputPosAndType.clear();
	    }
	    
	    public List<Pair<Integer, RelDataType>> getInputPosAndType() {
	    	return inputPosAndType;
	    }
	    
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
