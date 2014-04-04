package eu.stratosphere.sql.schema;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jline.internal.Log;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.AbstractSchema;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import eu.stratosphere.sql.schema.jsonAdapters.CustomSchemaAdapterAdapter;

/**
 * dispatcher for all JsonSchema based schema adapters.
 *
 * To implement a new JsonSchema-based adapter, you have to implement the
 * {@link JsonSchemaAdapter} interface. Then, register the class here.
 * 
 * Documentation for the Json-based schemas. (See also the {@link StratosphereSchemaFactory})
 * 
 * This class is doing the following:
 *  - it scans the directory the user is passing as (jsonSchemaStoreDir) for json files.
 *  	this is done in the (JsonSchema constructor)
 *  - for each file, it calls {@link JsonSchema#parseFile(File)}, there, we use Jackson to
 *  	map the JSON file into an internal object representation.
 *  - the {@link JsonSchema#parseFile(File)} method is reading only one property in the JSON file,
 *  	namely, the "type". It then looks up if there is an adapter registered for the type.
 *  - if there is an adapter, it calls {@link JsonSchemaAdapter#getTablesFromJson()} to extract the
 *  	table(s) from the the JsonNode. How the extraction happens specific to the adapter.
 *  
 *  
 *  How to register a {@link JsonSchemaAdapter}?
 *  	JsonSchemaAdapters can register themselves using the {@link JsonSchema#registerAdapter(JsonSchemaAdapter)} 
 *  	method. The method will call getTypeString() method on the adapter to get the "type" string.
 *  
 *
 */
public class JsonSchema extends AbstractSchema {

	public static Map<String, JsonSchemaAdapter> adapters = new HashMap<String, JsonSchemaAdapter>();
	
	public static String JSON_FILE_PATH_KEY = "jsonFilePath";
	public static String JSON_TYPE_KEY = "type";

	private Map<String, Table> tableMap;
	private TableAdder adder;

	public static void registerAdapter(JsonSchemaAdapter adapter) {
		final String type = adapter.getTypeString();
		Log.info("Registering schema adapter for '"+type+"'");
		adapters.put(type, adapter);
	}
	static {
		try {
			Class.forName("eu.stratosphere.sql.schema.jsonAdapters.CSVSchemaAdapter");
			Class.forName("eu.stratosphere.sql.schema.jsonAdapters.AvroSchemaAdapter");
			Class.forName("eu.stratosphere.sql.schema.jsonAdapters.CustomSchemaAdapterAdapter");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error loading adapter class",e);
		}
	}

	public JsonSchema(SchemaPlus incoming, File jsonSchemaStoreDir) {
		if(!jsonSchemaStoreDir.exists()) {
			throw new RuntimeException("Schema repository directory "+jsonSchemaStoreDir.getAbsolutePath()+" does not exist");
		}
		JsonSchemaUtils.filePathVariables.put("schema.dir", jsonSchemaStoreDir.getAbsolutePath());

		tableMap = new HashMap<String, Table>();
		adder = new TableAdderImpl(tableMap);

		File[] jsonFiles = jsonSchemaStoreDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".json");
			}
		});
		List<JsonNode> files = new ArrayList<JsonNode>();
		
		for(File file : jsonFiles) {
			try {
				files.add( parseFile(file) );
			} catch (Exception e) {
				throw new RuntimeException("Error while parsing file "+file, e);
			}
		}
		
		// Sort the files so that those with type=="jsonAdapter" are first.
		Collections.sort(files, new Comparator<JsonNode>() {
			@Override
			public int compare(JsonNode o1, JsonNode o2) {
				if(o1.get(JSON_TYPE_KEY).asText().equals(CustomSchemaAdapterAdapter.TYPE_STRING) ) {
					return -1;
				} else {
					return 1;
				}
			}
		});
		// call getTablesFromJson for each adapter
		for(JsonNode rootNode : files) {
			File file = new File(rootNode.get(JSON_FILE_PATH_KEY).asText());
			// get adapter for type.
			String type = rootNode.get(JSON_TYPE_KEY).asText();
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
		
	}

	/**
	 * Parses a Json file.
	 * @param file
	 */
	private ObjectNode parseFile(File file) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.readValue(file, ObjectNode.class);
		rootNode.put(JSON_FILE_PATH_KEY, file.getAbsolutePath());
		JsonNode typeNode = rootNode.get(JSON_TYPE_KEY);
		if(typeNode == null) {
			Log.warn("Json schema in file "+file.getAbsolutePath()+" is missing a type definition. Ignoring schema");
			return null;
		}
		return rootNode;
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
