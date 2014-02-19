package eu.stratosphere.sql;

import java.io.File;

import org.eigenbase.reltype.RelDataType;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;

public class FakeItTillYouMakeIt implements Function1<SchemaPlus, Schema> {

	@Override
	public Schema apply(SchemaPlus incoming) {
		CsvTableFactory fac = new CsvTableFactory();
		RelDataType type = new StratosphereRelType();
		Table tbl = fac.create(incoming,"tbl", null, type);
		// incoming.add("tbl", tbl);
		Schema schema = new CsvSchema(incoming, "tbl", new File("sales/"), false);
		incoming.add("tbl", tbl);
		return incoming.add(schema);
		//return 
	//	throw new RuntimeException("faked too much");
	}

}
