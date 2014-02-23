package eu.stratosphere.sql;

import java.io.PrintWriter;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.ConnectionConfig.Lex;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.RelWriterImpl;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.sql.SqlExplainLevel;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableSet;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.sql.relOpt.StratosphereRel;
import eu.stratosphere.sql.relOpt.StratosphereSqlProjection;
import eu.stratosphere.sql.rules.StratosphereProjectionRule;
import eu.stratosphere.sql.rules.StratosphereRuleSet;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;



public class Launcher  {

	public Launcher() {
//		StratosphereContext ctx = new StratosphereContext();
//		String sql = "SELECT * FROM DEPTS";
//		Type elementType = PactRecord.class;
//		OptiqPrepare.PrepareResult<PactRecord> prepared = new OptiqPrepareImpl()
//		        .prepareSql(ctx, sql, null, elementType, -1);
//		DataContext dataContext = new StratosphereContext();
//		Enumerator<PactRecord> enumerator = prepared.enumerator(dataContext);
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Function1<SchemaPlus, Schema> schemaFactory = new FakeItTillYouMakeIt();
		SqlStdOperatorTable operatorTable = SqlStdOperatorTable.instance();
		StratosphereRuleSet ruleSets = new StratosphereRuleSet( ImmutableSet.of(
	//		(RelOptRule) DataSourceRule.INSTANCE,
			(RelOptRule) StratosphereProjectionRule.INSTANCE
				
		//	TableAccessRule.INSTANCE,
		//	RemoveTrivialProjectRule.INSTANCE 
//			
//			 ExpandConversionRule.INSTANCE,
//		      SwapJoinRule.INSTANCE,
//		      RemoveDistinctRule.INSTANCE,
//		      UnionToDistinctRule.INSTANCE,
//		      RemoveTrivialProjectRule.INSTANCE,
//		      RemoveTrivialCalcRule.INSTANCE,
//		      RemoveSortRule.INSTANCE,
//		
//		      TableAccessRule.INSTANCE, //
//		      MergeProjectRule.INSTANCE, //
//		      PushFilterPastProjectRule.INSTANCE, //
//		      PushFilterPastJoinRule.FILTER_ON_JOIN, //
//		      RemoveDistinctAggregateRule.INSTANCE, //
//		      ReduceAggregatesRule.INSTANCE, //
//		      SwapJoinRule.INSTANCE, //
//		      PushJoinThroughJoinRule.RIGHT, //
//		      PushJoinThroughJoinRule.LEFT, //
//		      PushSortPastProjectRule.INSTANCE //
		));
		
		Planner planner = Frameworks.getPlanner(Lex.MYSQL, schemaFactory, operatorTable, ruleSets);
//		String sql = "SELECT a.cnt "
//				+ "FROM (SELECT COUNT(*) AS cnt FROM tbl GROUP BY NAME) AS a, tbl  "
//				+ "WHERE a.cnt = tbl.DEPTNO "
//				+ "ORDER BY a.cnt ASC LIMIT 2";
		String sql = "SELECT customerName, customerId, customerId, customerId FROM tbl";
		System.err.println("Sql = "+sql);
		SqlNode root = planner.parse(sql);
		SqlNode validated = planner.validate(root);
		RelNode rel = planner.convert(validated);
		
		// print out logical tree
		System.err.println("Got rel = "+rel);
		PrintWriter p = new PrintWriter(System.out);
		RelWriter pw = new RelWriterImpl(p,SqlExplainLevel.ALL_ATTRIBUTES, true);
		rel.explain(pw);
		
		// set high debugging level for optimizer
//		EigenbaseTrace.getPlannerTracer().setLevel(Level.ALL);
// 	  	ConsoleHandler handler = new ConsoleHandler();
//        handler.setLevel(Level.ALL);
//        EigenbaseTrace.getPlannerTracer().addHandler(handler);
	    
		// call optimizer? with own rules?
		RelNode convertedRelNode = planner.transform(0, planner.getEmptyTraitSet().plus(StratosphereRel.CONVENTION), rel);
		
		System.err.println("Optimizer "+ convertedRelNode);
		convertedRelNode.explain(pw);
		if(convertedRelNode instanceof StratosphereSqlProjection) {
			StratosphereSqlProjection stratoProj = ((StratosphereSqlProjection) convertedRelNode);
			
			Operator stratoRoot = stratoProj.getStratosphereOperator();
			System.err.println("Strato Root Op "+ stratoRoot);
			Class<? extends Value>[] fields = stratoProj.getFields();
			
			FileDataSink out = new FileDataSink(new CsvOutputFormat("\n", ",", fields), "file:///home/robert/Projekte/ozone/stratosphere-sql/simple.out", stratoRoot, "Sql Result");
			Plan plan = new Plan(out, "Stratosphere SQL: "+sql);
			
			LocalExecutor.execute(plan);
		}
//		printLogo();
//		Launcher l = new Launcher();
	}
	
}