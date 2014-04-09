package eu.stratosphere.sql.schema.jsonAdapters;

import java.io.File;
import eu.stratosphere.sql.schema.AvroStratosphereTable;
import eu.stratosphere.sql.schema.JsonSchema;
import eu.stratosphere.sql.schema.JsonSchemaAdapter;
import eu.stratosphere.sql.schema.SchemaAdapterException;
import org.apache.commons.io.FilenameUtils;
import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sql.schema.JsonSchema.TableAdder;

public class AvroSchemaAdapter implements JsonSchemaAdapter {
    static {
        JsonSchema.registerAdapter(new AvroSchemaAdapter());
    }

    @Override
    public String getTypeString() {
        return "avro";
    }

    @Override
    public void getTablesFromJson(JsonNode rootNode, TableAdder tableAdder, File file) throws SchemaAdapterException {
        System.err.println("Parsing JSON schema from "+rootNode);
        final String name = FilenameUtils.removeExtension(file.getName());
        AvroStratosphereTable table = new AvroStratosphereTable(rootNode, name);
        tableAdder.addTable(name, table);
    }

}
