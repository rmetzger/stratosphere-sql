package eu.stratosphere.sql.relOpt;

import java.util.List;
import java.util.Map;

import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;

import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

public class StratosphereDataSource  extends TableAccessRelBase implements StratosphereRel {

	private String filePath;
	private String columnDelimiter;
	private String rowDelimiter;
	private String tableName;
	private RelDataType rowType;

	
	public StratosphereDataSource(
		      RelOptCluster cluster,
		      RelOptTable table) {
		    super(
		        cluster,
		        cluster.traitSetOf(StratosphereRel.CONVENTION),
		        table);
		  }
	
	public StratosphereDataSource(
		      RelOptCluster cluster,
		      RelOptTable table,
		      String columnDelimiter,
		      String rowDelimiter,
		      String filePath,
		      String tableName,
		      RelDataType rowType) {
		    super(
		        cluster,
		        cluster.traitSetOf(StratosphereRel.CONVENTION),
		        table);
		    this.columnDelimiter = columnDelimiter;
		    this.rowDelimiter = rowDelimiter;
		    this.filePath = filePath;
		    this.tableName = tableName;
		    this.rowType = rowType;
		  }

/*	protected StratosphereDataSource(RelOptCluster cluster, RelTraitSet traits,
			RelOptTable table) {
		super(cluster, traits, table);
	} */

	@Override
	public Operator getStratosphereOperator() {
		//was before:
		//FileDataSource src = new FileDataSource(new CsvInputFormat(IntValue.class, StringValue.class), "file:///home/camelia2/stratosphere_sql/stratosphere-sql-1/simple.csv");

		//here we use the delimiters set in the json schema
		List<RelDataTypeField> fieldList = rowType.getFieldList();
		int position = 0;
		FileDataSource src = new FileDataSource(new CsvInputFormat(), "file://" + filePath, tableName);
		
		//it needs to loose a backslash
		if(rowDelimiter.equals("\\n"))
			rowDelimiter = "\n";
		
		CsvInputFormat.configureRecordFormat(src)
        	.recordDelimiter(rowDelimiter)
        	.fieldDelimiter(columnDelimiter.charAt(0));  
		
				
		for (RelDataTypeField field : fieldList){
			
			if(field.getType().toString().equals("INTEGER")) {
				CsvInputFormat.configureRecordFormat(src).field(IntValue.class, position);
				System.err.println("INT FIELD " + field.getName()+ " of type:" + field.getType() + " position:" + position);
			}
			else if(field.getType().toString().startsWith("VARCHAR")) {
				CsvInputFormat.configureRecordFormat(src).field(StringValue.class, position);
				System.err.println("VARCHAR FIELD " + field.getName()+ " of type:" + field.getType() + " position:" + position);
			}
			position += 1;
		}
		
		

		return src;
	}
	
}
