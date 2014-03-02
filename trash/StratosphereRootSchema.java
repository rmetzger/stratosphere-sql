package eu.stratosphere.sql;

import java.util.Map;

import org.eigenbase.reltype.RelDataType;

import eu.stratosphere.sql.schema.CsvTable;
import eu.stratosphere.sql.schema.CsvTableFactory;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;

public class StratosphereRootSchema extends MapSchema {

	public StratosphereRootSchema(QueryProvider queryProvider,
			JavaTypeFactory typeFactory) {
		super(null, queryProvider, typeFactory, "root", Expressions.parameter(
				Object.class, "root"));
		
	}

	@Override
	public <E> Table<E> getTable(String name, Class<E> elementType) {
		System.err.println("called for "
				+name);
		Map<String, Object> map;
		RelDataType rowType;
		return new CsvTableFactory().create(this, "DEPTS", map, rowType);
	}
}
