package eu.stratosphere.sql.schema;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.SchemaBuilder.FieldTypeBuilder;
import org.codehaus.jackson.JsonNode;

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
	 * Parses field types
	 * Supports:
	 * VARCHAR
	 * VARCHAR(15)
	 * DECIMAL
	 * DECIMAL(5,2)
	 *
	 */
	public static FieldType parseFieldType(String fieldType) {
		String pattern = "(.*)(\\d+)(.*)";

		FieldType ret = new FieldType();
		// Create a Pattern object
		Pattern r = Pattern.compile(pattern);

		// Now create matcher object.
		Matcher m = r.matcher(fieldType);
		if (m.find()) {
			ret.name = m.group(0);
			if(m.groupCount() > 1) {
				ret.arg1 = Integer.valueOf(m.group(1));
			}
			if(m.groupCount() > 2) {
				ret.arg2 = Integer.valueOf(m.group(2));
			}
		} else {
			throw new SchemaAdapterException("Unable to parse field type "+fieldType);
		}

		return ret;
	}


	public static class FieldType {
		public String name;
		// min_value represents "unset". This may become a bug some day.
		public int arg1 = Integer.MIN_VALUE;
		public int arg2 = Integer.MIN_VALUE;
	}
}
