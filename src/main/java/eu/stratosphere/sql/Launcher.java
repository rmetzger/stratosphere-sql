package eu.stratosphere.sql;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.ConnectionConfig.Lex;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.rel.rules.TableAccessRule;
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
			RemoveTrivialProjectRule.INSTANCE
		));
		Planner planner = Frameworks.getPlanner(Lex.MYSQL, schemaFactory, operatorTable, ruleSets);
		SqlNode root = planner.parse("SELECT * FROM tbl");
		SqlNode validated = planner.validate(root);
		RelNode rel = planner.convert(validated);
		System.err.println("Got rel = "+rel);
		
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