package eu.stratosphere.sql;

import java.io.FileReader;
import java.io.IOException;
import java.io.File;
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

import eu.stratosphere.sql.relOpt.CSVStratosphereDataSource;
import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;


public class StratosphereTable implements TranslatableTable {

	public RelDataType rowType;
	public String primaryKey;
	public String filePath;
	public String columnDelimiter = ","; //default is assumed to be comma
	public String rowDelimiter = "\n"; //default is assumed to be newline
	public String jsonFileName;
	
	
	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {		
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

		String tableName = jsonFileName.substring(jsonFileName.lastIndexOf("/"));
		if(filePath.endsWith(".csv")){
			return new CSVStratosphereDataSource(context.getCluster(), relOptTable, columnDelimiter, rowDelimiter, filePath, tableName, rowType);
		}
		else{
			//return new StratosphereDataSource(context.getCluster(), relOptTable);
			System.err.println("file format not yet supported");
			return null;
		}
		
	} 

}
