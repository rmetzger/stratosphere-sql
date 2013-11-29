package eu.stratosphere.sql;

import java.util.Iterator;

import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;


public class Launcher  {

	public Launcher() {
		
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