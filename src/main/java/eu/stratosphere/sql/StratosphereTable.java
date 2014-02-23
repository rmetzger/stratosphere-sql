package eu.stratosphere.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactory.FieldInfo;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Pair;

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
			List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<Map.Entry<String, RelDataType>>();
			Map.Entry<String, RelDataType> first = Pair.of("customerId", typeFactory.createSqlType(SqlTypeName.INTEGER));
			Map.Entry<String, RelDataType> second = Pair.of("customerName", typeFactory.createSqlType(SqlTypeName.VARCHAR));
			fieldList.add(first);
			fieldList.add(second);
			rowType = typeFactory.createStructType(fieldList);
					//typeFactory.createSqlType(SqlTypeName.ROW);
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
