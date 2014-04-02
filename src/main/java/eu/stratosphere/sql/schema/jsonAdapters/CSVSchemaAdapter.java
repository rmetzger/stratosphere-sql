package eu.stratosphere.sql.schema.jsonAdapters;

import java.io.File;

import org.apache.commons.io.FilenameUtils;
import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sql.schema.CSVStratosphereTable;
import eu.stratosphere.sql.schema.JsonSchema;
import eu.stratosphere.sql.schema.JsonSchema.TableAdder;
import eu.stratosphere.sql.schema.JsonSchemaAdapter;
import eu.stratosphere.sql.schema.SchemaAdapterException;

public class CSVSchemaAdapter implements JsonSchemaAdapter {

	static {
		JsonSchema.registerAdapter(new CSVSchemaAdapter());
	}

	@Override
	public String getTypeString() {
		return "csv";
	}



	@Override
	public void getTablesFromJson(JsonNode rootNode, TableAdder tableAdder, File file) throws SchemaAdapterException {
		System.err.println("Parsing JSON schema from "+rootNode);
		final String name = FilenameUtils.removeExtension(file.getName());
		CSVStratosphereTable table = new CSVStratosphereTable(rootNode, name);
		tableAdder.addTable(name, table);
	}

}
