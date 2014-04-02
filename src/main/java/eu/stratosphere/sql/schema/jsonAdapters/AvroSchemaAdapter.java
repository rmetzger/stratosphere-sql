package eu.stratosphere.sql.schema.jsonAdapters;

import java.io.File;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sql.schema.JsonSchema.TableAdder;
import eu.stratosphere.sql.schema.JsonSchema;
import eu.stratosphere.sql.schema.JsonSchemaAdapter;
import eu.stratosphere.sql.schema.SchemaAdapterException;

public class AvroSchemaAdapter implements JsonSchemaAdapter {

	static {
		JsonSchema.registerAdapter(new AvroSchemaAdapter());
	}

	@Override
	public String getTypeString() {
		return "avro";
	}

	@Override
	public void getTablesFromJson(JsonNode rootNode, TableAdder tableAddedImpl,
			File file) throws SchemaAdapterException {
		// TODO Auto-generated method stub

	}

}
