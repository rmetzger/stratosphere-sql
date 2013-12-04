package eu.stratosphere.sql;

import java.lang.reflect.Type;

import eu.stratosphere.pact.common.type.PactRecord;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;



public class Launcher  {

	public Launcher() {
		StratosphereContext ctx = new StratosphereContext();
		String sql = "SELECT * FROM TABLENAME";
		Type elementType = PactRecord.class;
		OptiqPrepare.PrepareResult<PactRecord> prepared = new OptiqPrepareImpl()
		        .prepareSql(ctx, sql, null, elementType, -1);
		DataContext dataContext = new StratosphereContext();
		Enumerator<PactRecord> enumerator = prepared.enumerator(dataContext);
		
	}
	
	public static void main(String[] args) throws Exception {
		printLogo();
		Launcher l = new Launcher();
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