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
import org.eigenbase.rel.rules.MergeProjectRule;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.rel.rules.PushFilterPastProjectRule;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.rel.rules.PushSortPastProjectRule;
import org.eigenbase.rel.rules.ReduceAggregatesRule;
import org.eigenbase.rel.rules.RemoveDistinctAggregateRule;
import org.eigenbase.rel.rules.RemoveDistinctRule;
import org.eigenbase.rel.rules.RemoveSortRule;
import org.eigenbase.rel.rules.RemoveTrivialCalcRule;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.rel.rules.SwapJoinRule;
import org.eigenbase.rel.rules.TableAccessRule;
import org.eigenbase.rel.rules.UnionToDistinctRule;
import org.eigenbase.relopt.volcano.AbstractConverter.ExpandConversionRule;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableSet;



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
			TableAccessRule.INSTANCE,
			RemoveTrivialProjectRule.INSTANCE,
			
			 ExpandConversionRule.INSTANCE,
		      SwapJoinRule.INSTANCE,
		      RemoveDistinctRule.INSTANCE,
		      UnionToDistinctRule.INSTANCE,
		      RemoveTrivialProjectRule.INSTANCE,
		      RemoveTrivialCalcRule.INSTANCE,
		      RemoveSortRule.INSTANCE,
		
		      TableAccessRule.INSTANCE, //
		      MergeProjectRule.INSTANCE, //
		      PushFilterPastProjectRule.INSTANCE, //
		      PushFilterPastJoinRule.FILTER_ON_JOIN, //
		      RemoveDistinctAggregateRule.INSTANCE, //
		      ReduceAggregatesRule.INSTANCE, //
		      SwapJoinRule.INSTANCE, //
		      PushJoinThroughJoinRule.RIGHT, //
		      PushJoinThroughJoinRule.LEFT, //
		      PushSortPastProjectRule.INSTANCE //
		));
		Planner planner = Frameworks.getPlanner(Lex.MYSQL, schemaFactory, operatorTable, ruleSets);
		String sql = "SELECT a.cnt "
				+ "FROM (SELECT COUNT(*) AS cnt FROM tbl GROUP BY NAME) AS a, tbl  "
				+ "WHERE a.cnt = tbl.DEPTNO "
				+ "ORDER BY a.cnt ASC LIMIT 2";
		System.err.println("Sql = "+sql);
		SqlNode root = planner.parse(sql);
		SqlNode validated = planner.validate(root);
		RelNode rel = planner.convert(validated);
		System.err.println("Got rel = "+rel);
		
		PrintWriter p = new PrintWriter(System.out);
		RelWriter pw = new RelWriterImpl(p);
		rel.explain(pw);
		
//		printLogo();
//		Launcher l = new Launcher();
	}
	
	
	private static void printLogo() {
		// gen by http://bigtext.org/
		System.out.println(
			"o-o    o           o               o                     o-o   o-o  o    \n"+
			"|      |           |               |                    |     o   o |    \n"+
			" o-o  -o- o-o  oo -o- o-o o-o o-o  O--o o-o o-o o-o      o-o  |   | |    \n"+
			"    |  |  |   | |  |  | |  \\  |  | |  | |-' |   |-'         | o   O |    \n"+
			"o--o   o  o   o-o- o  o-o o-o O-o  o  o o-o o   o-o     o--o   o-O\\ O---o\n"+
			"                              |                                          \n"+
			"                              o                                          \n"
		);
	}
}