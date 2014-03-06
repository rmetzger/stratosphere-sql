package eu.stratosphere.sql;

import java.io.File;

import org.eigenbase.reltype.RelDataType;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;

public class StratosphereSchemaFactory implements Function1<SchemaPlus, Schema> {

	@Override
	public Schema apply(SchemaPlus incoming) {

		Schema schema = new CsvSchema(incoming, "mySchema", new File("jsonSchemas/"), false);
		return incoming.add(schema);

	}

}
