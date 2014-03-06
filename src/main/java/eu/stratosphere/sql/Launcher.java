package eu.stratosphere.sql;

import java.io.PrintWriter;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.ConnectionConfig.Lex;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.RelWriterImpl;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;

import com.google.common.collect.ImmutableSet;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.sql.relOpt.StratosphereRel;
import eu.stratosphere.sql.relOpt.StratosphereSqlProjection;
import eu.stratosphere.sql.rules.StratosphereFilterRule;
import eu.stratosphere.sql.rules.StratosphereJoinRule;
import eu.stratosphere.sql.rules.StratosphereProjectionRule;
import eu.stratosphere.sql.rules.StratosphereRuleSet;
import eu.stratosphere.types.Value;



public class Launcher  {
	
	public static Plan convertSQLToPlan(String sql) throws SqlParseException, ValidationException, RelConversionException {
		Function1<SchemaPlus, Schema> schemaFactory = new StratosphereSchemaFactory();
		SqlStdOperatorTable operatorTable = SqlStdOperatorTable.instance();
		StratosphereRuleSet ruleSets = new StratosphereRuleSet( ImmutableSet.of(
			(RelOptRule) StratosphereProjectionRule.INSTANCE,
			StratosphereFilterRule.INSTANCE,
			StratosphereJoinRule.INSTANCE
		));
		
		Planner planner = Frameworks.getPlanner(Lex.MYSQL, schemaFactory, operatorTable, ruleSets);

		System.err.println("Sql = "+sql);
		SqlNode root = planner.parse(sql);
		SqlNode validated = planner.validate(root);
		RelNode rel = planner.convert(validated);
		
		// print out logical tree
		System.err.println("Got rel = "+rel);
		PrintWriter p = new PrintWriter(System.out);
		RelWriter pw = new RelWriterImpl(p, SqlExplainLevel.ALL_ATTRIBUTES, true);
		rel.explain(pw);
		
		// set high debugging level for optimizer
//		EigenbaseTrace.getPlannerTracer().setLevel(Level.ALL);
// 	  	ConsoleHandler handler = new ConsoleHandler();
//        handler.setLevel(Level.ALL);
//        EigenbaseTrace.getPlannerTracer().addHandler(handler);
	    
		RelNode convertedRelNode = planner.transform(0, planner.getEmptyTraitSet().plus(StratosphereRel.CONVENTION), rel);
		
		System.err.println("Optimizer "+ convertedRelNode);
		convertedRelNode.explain(pw);
		Operator stratoRoot = null;
		Plan plan = null;
		System.err.println("Create Stratosphere Plan: ");
		if(convertedRelNode instanceof StratosphereSqlProjection) {
			StratosphereSqlProjection stratoProj = ((StratosphereSqlProjection) convertedRelNode);
			
			stratoRoot = stratoProj.getStratosphereOperator();
			System.err.println("Strato Root Op "+ stratoRoot);
			Class<? extends Value>[] fields = stratoProj.getFields();
			FileDataSink out = new FileDataSink(new CsvOutputFormat("\n", ",", fields), "file:///home/robert/Projekte/ozone/stratosphere-sql/simple.out", stratoRoot, "Sql Result");
			plan = new Plan(out, "Stratosphere SQL: "+sql);
		}
		if(plan == null) {
			throw new RuntimeException("Sql Conversion failed!");
		}
		return plan;
	}
	
	public static void main(String[] args) throws Exception {
		Plan plan = convertSQLToPlan("SELECT a.customerName, a.customerId, b.customerId "
				+ "FROM tbl a, tbl b WHERE (a.customerId = b.customerId) AND (a.customerId < 15)");
		// LocalExecutor.execute(plan);
	}
	
}