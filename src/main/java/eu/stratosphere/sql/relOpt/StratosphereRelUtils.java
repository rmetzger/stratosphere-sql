package eu.stratosphere.sql.relOpt;

import java.io.Serializable;
import java.util.List;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.type.SqlTypeName;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.sql.StratosphereSQLRuntimeException;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class StratosphereRelUtils {


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
		if(type.getSqlTypeName() == SqlTypeName.VARCHAR) {
			return StringValue.class;
		}
		throw new RuntimeException("Unsupported type "+type);
	}
	
	public static Operator openSingleInputOperator(List<RelNode> optiqInput) {
		Operator inputOp = null;
		if(optiqInput.size() == 1) {
			final RelNode optiqSingleInput = optiqInput.get(0);
			final StratosphereRel stratoRel = toStratoRel(optiqSingleInput);
			inputOp = stratoRel.getStratosphereOperator();
		} else {
			throw new StratosphereSQLRuntimeException("Multiple inputs not supported at this time");
		}
		return inputOp;
	}
	
	public static StratosphereRel toStratoRel(RelNode input) {
		if(input instanceof RelSubset) {
			System.err.println("Had to convert "+input+" to best?");
			input = ( (RelSubset) input).getBest();
		}
		if(!(input instanceof StratosphereRel)) {
			throw new StratosphereSQLRuntimeException("Input is not a StratosphereRel. It is "+input.getClass().getName());
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
		private static final long serialVersionUID = 1L;
		public Class<? extends Value> type;
		public int positionInRecord;
		public String varName;
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
