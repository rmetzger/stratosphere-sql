package eu.stratosphere.sql;

import java.io.File;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map.Entry;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.config.Lex;
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
import eu.stratosphere.api.common.io.ListOutputFormat;
import eu.stratosphere.api.common.operators.AbstractUdfOperator;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.sql.relOpt.StratosphereRel;
import eu.stratosphere.sql.relOpt.StratosphereSqlProjection;
import eu.stratosphere.sql.rules.StratosphereFilterRule;
import eu.stratosphere.sql.rules.StratosphereJoinRule;
import eu.stratosphere.sql.rules.StratosphereProjectionRule;
import eu.stratosphere.sql.rules.StratosphereRuleSet;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;



public class Launcher	{
	
	private File defaultSchemaDir = new File("src/main/resources/jsonSchemas/");
	private Function1<SchemaPlus, Schema> schemaFactory = new StratosphereSchemaFactory(defaultSchemaDir);
	private SqlStdOperatorTable operatorTable = SqlStdOperatorTable.instance();
	private StratosphereRuleSet ruleSets;
	Planner planner;
	
	
	final private static Launcher INSTANCE = new Launcher();
	private Launcher() { 
		ruleSets = new StratosphereRuleSet( ImmutableSet.of(
					(RelOptRule) StratosphereProjectionRule.INSTANCE,
					StratosphereFilterRule.INSTANCE,
					StratosphereJoinRule.INSTANCE
				));
		planner = Frameworks.getPlanner(Lex.MYSQL, schemaFactory, operatorTable, ruleSets);
	}
	public static Launcher getInstance() {
		return INSTANCE;
	}
	
	public Operator convertToOperator(String sql) throws SqlParseException, ValidationException, RelConversionException {
		System.err.println("Sql = "+sql);
		SqlNode root = planner.parse(sql);
		SqlNode validated = planner.validate(root);
		RelNode rel = planner.convert(validated);
		
		// print out logical tree
		PrintWriter p = new PrintWriter(System.out);
		RelWriter pw = new RelWriterImpl(p, SqlExplainLevel.ALL_ATTRIBUTES, true);
		rel.explain(pw);
		
		
		RelNode convertedRelNode = planner.transform(0, planner.getEmptyTraitSet().plus(StratosphereRel.CONVENTION), rel);
		planner.close();
		planner.reset();
		System.err.println("Optimizer "+ convertedRelNode);
		convertedRelNode.explain(pw);
		Operator stratoRoot = null;
		Plan plan = null;
		System.err.println("Create Stratosphere Plan: ");
		if(convertedRelNode instanceof StratosphereSqlProjection) {
			StratosphereSqlProjection stratoProj = ((StratosphereSqlProjection) convertedRelNode);
			
			stratoRoot = stratoProj.getStratosphereOperator();
			return stratoRoot;
		}
		throw new RuntimeException("Fix me, its obvious");
	}
	// smallest Java class ever.
	public static class Pair<K,V> { public K k; public V v; }
	
	public Pair<Plan, Collection<Record>> convertToPlanWithCollection(String sql) {
		Operator stratoRoot;
		try {
			stratoRoot = convertToOperator(sql);
		} catch (Exception e) {
			throw new RuntimeException("Some Sql exception ", e);
		}
		
		System.err.println("Strato Root Op "+ stratoRoot);
		//Class<? extends Value>[] fields = stratoProj.getFields();
	//	FileDataSink out = new FileDataSink(new CsvOutputFormat("\n", ",", fields), "file://"+ System.getProperty("user.dir")+"//simple.out", stratoRoot, "Sql Result");
		ListOutputFormat collOut = new ListOutputFormat();
		GenericDataSink out = new GenericDataSink(collOut);
		out.setInput(stratoRoot);
		out.setDegreeOfParallelism(1);
		Plan plan = new Plan(out, "Stratosphere SQL. Query: "+sql);
		Pair<Plan, Collection<Record>> p = new Pair<Plan, Collection<Record>>();
		p.k = plan;
	//	p.v = coll;
		return p;
	}
	
	public Plan convertSQLToPlan(String sql)  {
		// I'm a bit sorry for this code
		return convertToPlanWithCollection(sql).k;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO: OUTDATED
		Launcher l = Launcher.getInstance();
		Plan plan = l.convertSQLToPlan("SELECT depName, customerId, customerId, customerId "
				+ "FROM customer WHERE ( customerId = 2 OR customerId = 3 OR customerId=3 ) AND (customerId < 15)");
		LocalExecutor.execute(plan);
		
		//Plan plan = convertSQLToPlan("SELECT COUNT(*) FROM tbl GROUP BY customerName");
		
		//Plan plan = convertSQLToPlan("SELECT SUBSTRING(customerName, 1, 10), SUM(customerId) FROM tbl GROUP BY SUBSTRING(customerName, 1, 10)");
				
		
		// LocalExecutor.execute(plan);
	}
	
}