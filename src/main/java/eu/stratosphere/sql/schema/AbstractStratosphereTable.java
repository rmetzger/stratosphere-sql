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

import eu.stratosphere.sql.relOpt.StratosphereCSVDataSource;

public abstract class AbstractStratosphereTable implements TranslatableTable, QueryableTable {

//	private RelDataType rowType;
//	//CHECKSTYLE:OFF
////	public String primaryKey;
//	public String filePath;
//	public String columnDelimiter = ","; //default is assumed to be comma
//	public String rowDelimiter = "\n"; //default is assumed to be newline
//	public String jsonFileName;
//	//CHECKSTYLE:ON


//	@Override
//	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
//		return this.rowType;
//	}
//
//	public void setRowType (RelDataType relData) {
//		this.rowType = relData;
//	}

//	@Override
//	public Statistic getStatistic() {
//		return Statistics.UNKNOWN;
//	}

	@Override
	public TableType getJdbcTableType() {
		return TableType.TABLE;
	}

//	@Override
//	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
//
//		String tableName = jsonFileName.substring(jsonFileName.lastIndexOf("/"));
//		if(filePath.endsWith(".csv")){
//			return new StratosphereCSVDataSource(context.getCluster(), relOptTable, columnDelimiter, rowDelimiter, filePath, tableName, rowType);
//		} else{
//			//return new StratosphereDataSource(context.getCluster(), relOptTable);
//			System.err.println("file format not yet supported");
//			return null;
//		}
//
//	}

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
