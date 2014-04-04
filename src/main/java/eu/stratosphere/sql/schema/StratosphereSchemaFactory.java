package eu.stratosphere.sql.schema;

import java.io.File;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

/**
 * Stratosphere SQL supports multiple kinds of schema providers.
 * 
 * JSON SCHEMA 
 * 		One is the {@link JsonSchema}. It requires a directory as input and scans it for
 * 		.json files describing tables. The table definition differ in their type. 
 * 			- One possible type is "csv", which means the input file is understood as a .csv file.
 * 			The user has to configure the delimters and fields of the csv file.
 * 			- Another type is "avro". It only requires the path to the avro file since
 * 			avro files have their schema self-contained.
 * 
 * HCATALOG Schema
 * 		This schema is not yet implemented. It is accessing the schema information stored
 * 		in hive's metastore db.
 * 
 * Stratosphere DataSet Schema
 * 		This schema is not yet implemented. The idea here is to provide the schema of a 
 * 		Stratosphere {@link DataSet} as a SQL table. 
 *
 */
public class StratosphereSchemaFactory implements Function1<SchemaPlus, Schema> {

	private final File jsonSchemaStoreDir;
	public StratosphereSchemaFactory(File jsonSchemaStoreDir) {
		this.jsonSchemaStoreDir = jsonSchemaStoreDir;
	}
	@Override
	public Schema apply(SchemaPlus incoming) {
		Schema jsonSchema = new JsonSchema(incoming, jsonSchemaStoreDir);
		// add HCatalog schema here.
		// add StratosphereOperatorSchema here.
		return incoming.add("Stratosphere", jsonSchema);
	}

}
