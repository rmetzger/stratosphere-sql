package eu.stratosphere.sql.relOpt;

import java.io.Serializable;
import java.util.List;

import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.type.SqlTypeName;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.sql.StratosphereSQLException;
import eu.stratosphere.types.DateValue;
import eu.stratosphere.types.DecimalValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class StratosphereRelUtils {
	private StratosphereRelUtils() {}

	public static Value newValue(RelDataType in) {
		if(in.getSqlTypeName() == SqlTypeName.INTEGER) {
			return new IntValue();
		}
		if(in.getSqlTypeName() == SqlTypeName.VARCHAR) {
			return new StringValue();
		}
		throw new RuntimeException("Unsupported type "+in);
	}

	public static Class<? extends Value> getTypeClass(RelDataType type) {
		if(type.getSqlTypeName() == SqlTypeName.INTEGER) {
			return IntValue.class;
		}
		if(type.getSqlTypeName() == SqlTypeName.VARCHAR || type.getSqlTypeName() == SqlTypeName.CHAR) {
			return StringValue.class;
		}
		if(type.getSqlTypeName() == SqlTypeName.DOUBLE) {
			return DoubleValue.class;
		}
		if(type.getSqlTypeName() == SqlTypeName.BIGINT) {
			return LongValue.class;
		}
		if(type.getSqlTypeName() == SqlTypeName.DECIMAL) {
			return DecimalValue.class;
		}
		if(type.getSqlTypeName() == SqlTypeName.DATE) {
			return DateValue.class;
		}
		throw new RuntimeException("Unsupported type "+type);
	}

	public static Class<? extends Key> getKeyTypeClass(RelDataType type) {
		final Class<? extends Value> val = getTypeClass(type);
		if(Key.class.isAssignableFrom(val) ) {
			return (Class<? extends Key>) val;
		}
		throw new RuntimeException("RelDataType is not a Key: "+type+" to val "+val);
	}

	public static Class<? extends Value> getTypeClass(Class clazz) {
		if(clazz == String.class) {
			return StringValue.class;
		}
		if(clazz == Integer.class) {
			return IntValue.class;
		}
		if(clazz == Double.class) {
			return DoubleValue.class;
		}
		if(clazz == Long.class) {
			return LongValue.class;
		}
		throw new RuntimeException("Unsupported class "+clazz);
	}

	public static Operator openSingleInputOperator(List<RelNode> optiqInput) {
		Operator inputOp = null;
		if(optiqInput.size() == 1) {
			final RelNode optiqSingleInput = optiqInput.get(0);
			final StratosphereRel stratoRel = toStratoRel(optiqSingleInput);
			inputOp = stratoRel.getStratosphereOperator();
		} else {
			throw new StratosphereSQLException("Multiple inputs not supported at this time");
		}
		return inputOp;
	}

	public static StratosphereRel toStratoRel(RelNode input) {
		if(!(input instanceof StratosphereRel)) {
			throw new StratosphereSQLException("Input is not a StratosphereRel. It is "+input.getClass().getName());
		}
		return (StratosphereRel) input;
	}

	public static String convertRexCallToJexlExpr(RexNode c) {
		StringBuffer sb = new StringBuffer();
		if(c instanceof RexCall) {
			RexCall call = (RexCall) c;
			sb.append("(");
			for(int i = 0; i < call.getOperands().size(); i++) {
				sb.append( convertRexCallToJexlExpr(call.getOperands().get(i)));
				if(i+1 <call.getOperands().size()) {
					switch(c.getKind()) {
						case AND:
							sb.append(" && ");
							break;
						case OR:
							sb.append(" || ");
							break;
						case EQUALS:
							sb.append(" == ");
							break;
						case LESS_THAN:
							sb.append(" < ");
							break;
						default:
							throw new RuntimeException("Unknown kind "+c.getKind());
					}
				}
			}
			sb.append(")");
		}
		// assignable variable
		if(c instanceof RexInputRef) {
			RexInputRef ref = (RexInputRef) c;
			return ref.getName();
		}
		if(c instanceof RexLiteral) {
			RexLiteral lit = (RexLiteral) c;
			return lit.getValue().toString();
		}
		return sb.toString();
	}

	public static class ExprVar implements Serializable {
		//CHECKSTYLE:OFF
		private static final long serialVersionUID = 1L;
		public Class<? extends Value> type;
		public int positionInRecord;
		public String varName;
		//CHECKSTYLE:ON
	}

	public static void getExprVarsFromRexCall(RexNode cond, List<ExprVar> result) {
		if(cond instanceof RexCall) {
			RexCall call = (RexCall) cond;
			for(int i = 0; i < call.getOperands().size(); i++) {
				getExprVarsFromRexCall(call.getOperands().get(i), result);
			}
			return;
		}
		// assignable variable
		if(cond instanceof RexInputRef) {
			RexInputRef ref = (RexInputRef) cond;
			for (ExprVar expr : result) {
				if(expr.varName.equals(ref.getName())) {
					// variable already created;
					return;
				}
			}
			ExprVar e = new ExprVar();
			e.varName = ref.getName();
			e.positionInRecord = ref.getIndex();
			e.type = getTypeClass(ref.getType());
			result.add(e);
			return;
		}
	}

}


