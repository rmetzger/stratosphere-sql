package eu.stratosphere.sql.schema;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import jline.internal.Log;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.AbstractSchema;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonSchema extends AbstractSchema {

	public static Map<String, JsonSchemaAdapter> adapters = new HashMap<String, JsonSchemaAdapter>();

	private Map<String, Table> tableMap;
	private TableAdder adder;

	public static void registerAdapter(JsonSchemaAdapter adapter) {
		adapters.put(adapter.getTypeString(), adapter);
	}
	static {
		try {
			Class.forName("eu.stratosphere.sql.schema.jsonAdapters.CSVSchemaAdapter");
			// add your custom schemas here.

			// TODO: (idea) add a way for "users" to register their own, external schema adapters.
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error loading adapter class",e);
		}
	}

	public JsonSchema(SchemaPlus incoming, File jsonSchemaStoreDir) {
		if(!jsonSchemaStoreDir.exists()) {
			throw new RuntimeException("Schema repository directory "+jsonSchemaStoreDir.getAbsolutePath()+" does not exist");
		}
		tableMap = new HashMap<String, Table>();
		adder = new TableAdderImpl(tableMap);

		File[] jsonFiles = jsonSchemaStoreDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".json");
			}
		});

		for(File file : jsonFiles) {
			try {
				parseFile(file);
			} catch (Exception e) {
				throw new RuntimeException("Error while parsing file "+file, e);
			}
		}
	}

	/**
	 * Parses a Json file.
	 * @param file
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
	private void parseFile(File file) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode rootNode = mapper.readValue(file, JsonNode.class);
		JsonNode typeNode = rootNode.get("type");
		if(typeNode == null) {
			Log.warn("Json schema in file "+file.getAbsolutePath()+" is missing a type definition. Ignoring schema");
			return;
		}
		String type = typeNode.asText();
		JsonSchemaAdapter adapterForType = adapters.get(type);
		if(adapterForType == null) {
			throw new RuntimeException("No adapter for type '"+type+"' available!");
		}
		try {
			adapterForType.getTablesFromJson(rootNode, adder, file);
		} catch(SchemaAdapterException sae) {
			throw new RuntimeException("Error while parsing file "+file.getAbsolutePath(), sae);
		}
	}

	@Override
	protected Map<String, Table> getTableMap() {
		return tableMap;
	}

	public static interface TableAdder {
		public void addTable(String name, AbstractStratosphereTable tbl);
	}
	public static class TableAdderImpl implements TableAdder {
		private Map<String, Table> tableMap;
		public TableAdderImpl(Map<String, Table> tableMap) {
			this.tableMap = tableMap;
		}
		@Override
		public void addTable(String name, AbstractStratosphereTable tbl) {
			Log.info("Registering table '"+name+"'");
			if(tableMap.put(name, tbl) != null) {
				throw new RuntimeException("Table "+name+" already registered");
			}
		}

	}
}
