package eu.stratosphere.sql.relOpt;

import java.util.List;

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;

import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.types.ByteValue;
import eu.stratosphere.types.CharValue;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.ShortValue;
import eu.stratosphere.types.StringValue;

public class StratosphereCSVDataSource extends StratosphereDataSource {

	private String filePath;
	private String columnDelimiter;
	private String rowDelimiter;
	private String tableName;
	private RelDataType rowType;
	
	public StratosphereCSVDataSource(RelOptCluster cluster, RelOptTable table) {
		super(cluster, table);
	}

	public StratosphereCSVDataSource(RelOptCluster cluster, RelOptTable table,
			String columnDelimiter, String rowDelimiter, String filePath,
			String tableName, RelDataType rowType) {
		super(cluster, table);
		this.columnDelimiter = columnDelimiter;
		this.rowDelimiter = rowDelimiter;
		this.filePath = filePath;
		this.tableName = tableName;
		this.rowType = rowType;
	}
	
	@Override
	public Operator getStratosphereOperator() {
		//here we use the delimiters set in the json schema
		List<RelDataTypeField> fieldList = rowType.getFieldList();
		int position = 0;
		FileDataSource src = new FileDataSource(new CsvInputFormat(), filePath, tableName);
		
		//it needs to loose a backslash
		if(rowDelimiter.equals("\\n")) {
			rowDelimiter = "\n";
		}
		
		CsvInputFormat.configureRecordFormat(src)
			.recordDelimiter(rowDelimiter)
			.fieldDelimiter(columnDelimiter.charAt(0));	
		
				
		for (RelDataTypeField field : fieldList) {
			if(field.getType().toString().equals("INTEGER")) {
				CsvInputFormat.configureRecordFormat(src).field(IntValue.class, position);
			} else if(field.getType().toString().equals("BIGINT")) {
				CsvInputFormat.configureRecordFormat(src).field(LongValue.class, position);
			} else if(field.getType().toString().equals("SMALLINT")) {
				CsvInputFormat.configureRecordFormat(src).field(ShortValue.class, position);
			} else if(field.getType().toString().equals("TINYINT")) {
				CsvInputFormat.configureRecordFormat(src).field(ByteValue.class, position);
			} else if(field.getType().toString().equals("FLOAT")) {
				CsvInputFormat.configureRecordFormat(src).field(FloatValue.class, position);
			} else if(field.getType().toString().equals("DOUBLE")) {
				CsvInputFormat.configureRecordFormat(src).field(DoubleValue.class, position);
			} else if(field.getType().toString().startsWith("CHAR")) {
				CsvInputFormat.configureRecordFormat(src).field(CharValue.class, position);
			} else if(field.getType().toString().startsWith("VARCHAR")) {
				CsvInputFormat.configureRecordFormat(src).field(StringValue.class, position);
			}
			
			position += 1;
		}
		
		

		return src;
	}

}