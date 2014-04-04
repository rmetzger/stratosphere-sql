package eu.stratosphere.sql.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.eigenbase.sql.type.SqlTypeName;

public class JsonSchemaUtils {

	public static JsonNode getField(JsonNode node, String fieldName) throws SchemaAdapterException {
		JsonNode field = node.get(fieldName);
		if(field == null) {
			throw new SchemaAdapterException("Missing field '"+fieldName+"'");
		}
		return field;
	}

	public static String getOptionalString(JsonNode node, String fieldName, String def) {
		JsonNode field = node.get(fieldName);
		if(field == null) {
			return def;
		} else {
			return field.asText();
		}
	}

	public static Character getOptionalChar(JsonNode node, String fieldName, char def) {
		JsonNode field = node.get(fieldName);
		if(field == null) {
			return def;
		} else {
			String val = field.asText();
			if(val.length() != 1) {
				throw new SchemaAdapterException("Field '"+fieldName+"' is expected to be a character");
			}
			return val.charAt(0); // happy autoboxing
		}
	}

	public static String getStringField(JsonNode rootNode, String string) {
		return getField(rootNode, string).asText();
	}

	/**
	 * Filename variables
	 */
	public static Map<String, String> filePathVariables = new HashMap<String, String>();
	static {
		filePathVariables.put("pwd", System.getProperty("user.dir"));
		filePathVariables.put("user.dir", System.getProperty("user.dir"));
		filePathVariables.put("home.dir", System.getProperty("user.home"));
		filePathVariables.put("home.name", System.getProperty("user.name"));
		filePathVariables.put("file.separator", System.getProperty("file.separator"));
	}
	public static String replaceFilenameVariables(String inText) {
		for(Map.Entry<String, String> replField : filePathVariables.entrySet()) {
			inText = inText.replaceAll("\\{\\{"+replField.getKey()+"\\}\\}", replField.getValue());
		}
		return inText;
	}

	/**
	 * Parses field types
	 * Supports:
	 * VARCHAR
	 * VARCHAR(15)
	 * DECIMAL
	 * DECIMAL(5,2)
	 *
	 */
	private final static Pattern pattern = Pattern.compile("([a-zA-Z]+)(\\(([1-9][0-9]*),?([1-9][0-9]*)?\\))?");
	public static FieldType parseFieldType(String fieldType) {

		FieldType ret = new FieldType();
		// Create a Pattern object

		// Now create matcher object.
		Matcher m = pattern.matcher(fieldType);
		if (m.find()) {
			//System.err.println(fieldType+" Groups "+m.groupCount()+" 0"+m.group(0)+" 1"+m.group(1)+" 2"+m.group(2)+" 3"+m.group(3)+" 4"+m.group(4));
			ret.name = SqlTypeName.valueOf(m.group(1).toUpperCase());
			if(m.group(3) != null) {
				ret.arg1 = Integer.valueOf(m.group(3));
			}
			if(m.group(4) != null) {
				ret.arg2 = Integer.valueOf(m.group(4));
			}
		} else {
			throw new SchemaAdapterException("Unable to parse field type "+fieldType);
		}

		return ret;
	}

	public static class FieldType {
		public SqlTypeName name;
		// min_value represents "unset". This may become a bug some day.
		public int arg1 = Integer.MIN_VALUE;
		public int arg2 = Integer.MIN_VALUE;
	}
}
