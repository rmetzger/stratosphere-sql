package eu.stratosphere.sql.utils;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sql.schema.JsonSchemaUtils;
import eu.stratosphere.sql.schema.JsonSchemaUtils.FieldType;

public class SchemaTests {


	@Test public void testTypeParser() {
		FieldType type = JsonSchemaUtils.parseFieldType("varchar");
		Assert.assertEquals(type.name, "VARCHAR");

		type = JsonSchemaUtils.parseFieldType("VARCHAR(15)");
		Assert.assertEquals(type.name, "VARCHAR");
		Assert.assertEquals(type.arg1, 15);

		type = JsonSchemaUtils.parseFieldType("DECIMAL");
		Assert.assertEquals(type.name, "DECIMAL");

		type = JsonSchemaUtils.parseFieldType("DECIMAL(5,2)");
		Assert.assertEquals(type.name, "DECIMAL");
		Assert.assertEquals(type.arg1, 5);
		Assert.assertEquals(type.arg2, 2);
	}
}
