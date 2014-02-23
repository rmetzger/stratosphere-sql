package eu.stratosphere.sql.optimizer;

import java.util.Collection;

import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.sql.Launcher;
import eu.stratosphere.sql.relOpt.StratosphereSqlFilter.StratosphereSqlFilterMapOperator;

public class JexlBooleanEvaluationTest {
	private static final JexlEngine jexl = new JexlEngine();
	static {
		jexl.setCache(512);
		jexl.setLenient(false);
		jexl.setSilent(false);
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
	
	@Test
	public void doIt() throws SqlParseException, ValidationException, RelConversionException {
		Plan p = Launcher.convertSQLToPlan("SELECT customerName, customerId, customerId, customerId "
				+ "FROM tbl "
				+ "WHERE ( ( customerId = 0 OR customerId = 3 OR customerId=3 ) AND (customerId = 0 AND customerId < 15)) OR (customerId = 16)");
		Collection<GenericDataSink> sinks = p.getDataSinks();
		GenericDataSink sink = sinks.iterator().next();
		SingleInputOperator sqlProjection = (SingleInputOperator) sink.getInputs().get(0);
		StratosphereSqlFilterMapOperator sqlFilter = (StratosphereSqlFilterMapOperator) ((UserCodeObjectWrapper)( (MapOperator) sqlProjection.getInputs().get(0)).getUserCodeWrapper() ).getInitialUserCodeObject();
		
		RexNode cond = sqlFilter.getCondition();

		String expression = convertRexCallToJexlExpr(cond);
		System.err.println("Expression '"+expression+"' from cond '"+cond+"'");
		Expression e = jexl.createExpression(expression);
		JexlContext context = new MapContext();
		context.set("$0", 0);
		Boolean res = (Boolean) e.evaluate(context);
		Assert.assertTrue(res);
		context.set("$0", 1);
		Assert.assertFalse( (Boolean) e.evaluate(context));
	}
}
