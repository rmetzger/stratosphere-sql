package eu.stratosphere.sql.optimizer;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.ConnectionConfig.Lex;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import eu.stratosphere.sql.FakeItTillYouMakeIt;
import eu.stratosphere.sql.relOpt.StratosphereRelUtils;
import eu.stratosphere.sql.rules.StratosphereFilterRule;
import eu.stratosphere.sql.rules.StratosphereProjectionRule;
import eu.stratosphere.sql.rules.StratosphereRuleSet;

public class JexlBooleanEvaluationTest {
	private static final JexlEngine jexl = new JexlEngine();
	static {
		jexl.setCache(512);
		jexl.setLenient(false);
		jexl.setSilent(false);
	}
	
	
	@Test
	public void doIt() throws SqlParseException, ValidationException, RelConversionException {
		String sql = "SELECT customerName, customerId, customerId, customerId "
				+ "FROM tbl "
				+ "WHERE ( ( customerId = 0 OR customerId = 3 OR customerId=3 ) AND (customerId = 0 AND customerId < 15)) OR (customerId = 16)";
		
		
		Function1<SchemaPlus, Schema> schemaFactory = new FakeItTillYouMakeIt();
		SqlStdOperatorTable operatorTable = SqlStdOperatorTable.instance();
		StratosphereRuleSet ruleSets = new StratosphereRuleSet( ImmutableSet.of(
			(RelOptRule) StratosphereProjectionRule.INSTANCE,
			StratosphereFilterRule.INSTANCE
		));
		
		Planner planner = Frameworks.getPlanner(Lex.MYSQL, schemaFactory, operatorTable, ruleSets);

		// 
		System.err.println("Sql = "+sql);
		SqlNode root = planner.parse(sql);
		SqlNode validated = planner.validate(root);
		RelNode rel = planner.convert(validated);
		
		FilterRel a = (FilterRel) rel.getInput(0);
		
		RexNode cond = a.getCondition();

		String expression = StratosphereRelUtils.convertRexCallToJexlExpr(cond);
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
