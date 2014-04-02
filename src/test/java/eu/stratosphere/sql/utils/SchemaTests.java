package eu.stratosphere.sql.utils;

import org.eigenbase.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sql.schema.JsonSchemaUtils;
import eu.stratosphere.sql.schema.JsonSchemaUtils.FieldType;

public class SchemaTests {


	@Test public void testTypeParser() {
		FieldType type = JsonSchemaUtils.parseFieldType("varchar");
		Assert.assertEquals(SqlTypeName.VARCHAR, type.name);

		type = JsonSchemaUtils.parseFieldType("DECIMAL(5,2)");
		Assert.assertEquals(SqlTypeName.DECIMAL, type.name);
		Assert.assertEquals(5, type.arg1);
		Assert.assertEquals(2, type.arg2);

		type = JsonSchemaUtils.parseFieldType("VARCHAR(15)");
		Assert.assertEquals(SqlTypeName.VARCHAR, type.name);
		Assert.assertEquals(15, type.arg1);
		Assert.assertEquals(Integer.MIN_VALUE, type.arg2);

		type = JsonSchemaUtils.parseFieldType("DECIMAL");
		Assert.assertEquals(SqlTypeName.DECIMAL, type.name );
		Assert.assertEquals(Integer.MIN_VALUE, type.arg1);
		Assert.assertEquals(Integer.MIN_VALUE, type.arg2);

	}
}
