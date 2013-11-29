package eu.stratosphere.sql;

import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;



public class Launcher  {

	public Launcher() {
//		AdapterContext ctx = new AdapterContext();
//		String sql = "SELECT * FROM TABLENAME";
//		Type elementType = Object[].class;
//		OptiqPrepare.PrepareResult<Object> prepared = new OptiqPrepareImpl()
//		        .prepareSql(ctx, sql, null, elementType, -1);
//		Object enumerable = prepared.getExecutable();
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