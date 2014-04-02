package eu.stratosphere.sql.schema;

import java.io.File;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sql.schema.JsonSchema.TableAdder;

/**
 * Each type for the JSON schema has to implement this adapter.
 * The types are identified by a type string.
 */
public interface JsonSchemaAdapter {
	/**
	 *
	 * @return a string to identify the type, such as "avro", "csv", or "parquet".
	 */
	public String getTypeString();

	/**
	 *
	 * @param rootNode the JsonNode containing the type-object.
	 * @param tableAdder add tables here
	 * @throws SchemaAdapterException
	 */
	public void getTablesFromJson(JsonNode rootNode, TableAdder tableAddedImpl, File file) throws SchemaAdapterException;
}
