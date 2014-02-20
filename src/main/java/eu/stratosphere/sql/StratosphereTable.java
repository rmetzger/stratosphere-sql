package eu.stratosphere.sql;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;

import eu.stratosphere.sql.relOpt.StratosphereDataSource;
import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;

public class StratosphereTable implements TranslatableTable {

	private RelDataType rowType;

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if(rowType == null) {
			rowType = typeFactory.createSqlType(SqlTypeName.ROW);
		}
		return this.rowType;
	}

	@Override
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}

	@Override
	public TableType getJdbcTableType() {
		return TableType.TABLE;
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		return new StratosphereDataSource(context.getCluster(), relOptTable);
	} 

}
