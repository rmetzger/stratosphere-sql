package eu.stratosphere.sql;

import java.io.File;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

public class StratosphereSchemaFactory implements Function1<SchemaPlus, Schema> {

	private final File jsonSchemaStoreDir;
	public StratosphereSchemaFactory(File jsonSchemaStoreDir) {
		this.jsonSchemaStoreDir = jsonSchemaStoreDir;
	}
	@Override
	public Schema apply(SchemaPlus incoming) {
		Schema schema = new CsvSchema(incoming, "mySchema", jsonSchemaStoreDir);
		return incoming.add("Stratosphere", schema);
	}

}
