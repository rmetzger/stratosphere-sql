package eu.stratosphere.sql.schema;

import java.lang.reflect.Type;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.QueryableTable;
import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;


public abstract class AbstractStratosphereTable implements TranslatableTable, QueryableTable {


	@Override
	public TableType getJdbcTableType() {
		return TableType.TABLE;
	}


	@Override
	public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
			SchemaPlus schema, String tableName) {
		return null;
	}

	@Override
	public Type getElementType() {
		return null;
	}

	@Override
	public Expression getExpression(SchemaPlus schema, String tableName,
			Class clazz) {
		return null;
	}

}
