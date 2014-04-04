package eu.stratosphere.sql.schema.jsonAdapters;

import java.io.File;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sql.schema.JsonSchema.TableAdder;
import eu.stratosphere.sql.schema.JsonSchema;
import eu.stratosphere.sql.schema.JsonSchemaAdapter;
import eu.stratosphere.sql.schema.JsonSchemaUtils;
import eu.stratosphere.sql.schema.SchemaAdapterException;

/**
 *
 */
public class CustomSchemaAdapterAdapter implements JsonSchemaAdapter {
	public static final String TYPE_STRING = "customJsonAdapter";
	
	static {
		JsonSchema.registerAdapter(new CustomSchemaAdapterAdapter());
	}
	
	
	@Override
	public String getTypeString() {
		return TYPE_STRING;
	}

	@Override
	public void getTablesFromJson(JsonNode rootNode, TableAdder tableAddedImpl,
			File file) throws SchemaAdapterException {
		String clazz = JsonSchemaUtils.getStringField(rootNode, "class");
		try {
			Class.forName(clazz);
		} catch (ClassNotFoundException e) {
			throw new SchemaAdapterException("Error loading class "+clazz, e);
		}
	}

}
