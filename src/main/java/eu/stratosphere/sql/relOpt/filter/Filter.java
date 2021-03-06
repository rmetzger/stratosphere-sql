package eu.stratosphere.sql.relOpt.filter;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.runtime.Utilities;

import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexExecutable;
import org.eigenbase.rex.RexExecutorImpl;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.stratosphere.sql.relOpt.StratosphereDataContext;
import eu.stratosphere.sql.relOpt.StratosphereRelUtils;
import eu.stratosphere.sql.relOpt.StratosphereRexUtils;
import eu.stratosphere.types.DateValue;
import eu.stratosphere.types.JavaValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.ReflectionUtil;

public class Filter implements Serializable {
	private static final long serialVersionUID = 1L;
	// "temporary", "client side" variables
	private transient RexNode condition;
	private transient RexBuilder rexBuilder;
	// temporary cluster side variables
	private transient Function1<DataContext, Object[]> function;

	// to-transfer data.
	private Set<StratosphereRexUtils.ProjectionFieldProperties> fields;
	private String source;

	public Filter() {

	}

	public void setCondition(RexNode condition) {
		this.condition = condition;
	}

	public void setRexBuilder(RexBuilder rexBuilder) {
		this.rexBuilder = rexBuilder;
	}

	/**
	 * Create the list of fields.
	 * @param rowType
	 */
	public void prepareShipping(RelDataType rowType) {
		Preconditions.checkNotNull(condition);
		Preconditions.checkNotNull(rexBuilder);

		StratosphereRexUtils.GetInputRefVisitor replaceInputRefsByExternalInputRefsVisitor = new StratosphereRexUtils.GetInputRefVisitor();
		condition.accept(replaceInputRefsByExternalInputRefsVisitor);

		final ImmutableList<RexNode> localExps = ImmutableList.of(condition);

		fields = new HashSet<StratosphereRexUtils.ProjectionFieldProperties>();
		int pos = 0;
		for(Pair<Integer, RelDataType> rexInput : replaceInputRefsByExternalInputRefsVisitor.getInputPosAndType() ) {
			StratosphereRexUtils.ProjectionFieldProperties field = new StratosphereRexUtils.ProjectionFieldProperties();
			field.fieldIndex = pos++;
			field.positionInInput = rexInput.getKey();
			field.inFieldType = StratosphereRelUtils.getTypeClass(rexInput.getValue());
			field.name = condition.toString();
			fields.add(field);
		}
		final RexExecutorImpl executor = new RexExecutorImpl(null);
		RexExecutable executable = executor.getExecutable(rexBuilder, localExps, rowType);
		System.err.println("Code: "+executable.getSource());
		this.source = executable.getSource();
	}

	public void prepareEvaluation() {
		try {
			this.function =  (Function1<DataContext, Object[]>) ClassBodyEvaluator.createFastClassBodyEvaluator(
				new Scanner(null, new StringReader(source)),
				RexExecutable.GENERATED_CLASS_NAME,
				Utilities.class,
				new Class[]{Function1.class , Serializable.class},
				getClass().getClassLoader());
		} catch (Exception e) {
			throw new RuntimeException("Error while compiling the generated code");
		}
	}

	// evaluate operator fields
	private transient Value[] valuesCache;
	private transient StratosphereDataContext dataContext;

	public boolean evaluate(Record record) {
		return evaluateTwo(record, null);
	}

	public boolean evaluateTwo(Record record, Record record2) {
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
			if(field.positionInInput < record.getNumFields()) {
				record.getFieldInto(field.positionInInput, valuesCache[field.fieldIndex]);
			} else {
				if(record2 == null) {
					throw new RuntimeException("Second record not set. Filter not configured correctly");
				}
				record2.getFieldInto(field.positionInInput-record.getNumFields(), valuesCache[field.fieldIndex]);
			}
			Object value = ((JavaValue) valuesCache[field.fieldIndex]).getObjectValue();
			if(field.inFieldType == DateValue.class) {
				System.err.println("Converting the date ("+value+") to int");
				value = (int) ( (Date) value).getTime(); // cast to int.
			}
			dataContext.set(field.positionInInput, value );
		}
		Object[] result = function.apply(dataContext);
		for(Object o : result) {
			System.err.println("result = "+o);
		}
		return (Boolean) result[0];
	}
}
