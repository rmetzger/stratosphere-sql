package eu.stratosphere.sql.schema;

import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;

import org.codehaus.jackson.JsonNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvInputFormat.ConfigBuilder;

public class CSVStratosphereTable extends AbstractStratosphereTable {

	private RelDataType rowType = null;
	FileDataSource fileSrcOperator;
	private ConfigBuilder builder;
	private JsonNode rootNode;

	public CSVStratosphereTable(JsonNode rootNode, String name) throws SchemaAdapterException {
		this.rootNode = rootNode;
		// parse "metadata"

		String recordDelimiter = JsonSchemaUtils.getOptionalString(rootNode, "rowDelimiter", String.valueOf('\n'));
		builder.recordDelimiter(recordDelimiter);

		char fieldDelimiter = JsonSchemaUtils.getOptionalChar(rootNode, "fieldDelimiter", ',');
		builder.fieldDelimiter(fieldDelimiter);

		JsonNode pathNode = JsonSchemaUtils.getField(rootNode, "filePath");
		fileSrcOperator = new FileDataSource(new CsvInputFormat(), pathNode.asText(), "CsvInput: "+name);
		builder = CsvInputFormat.configureRecordFormat(fileSrcOperator);
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if(rowType == null) {
			parseRowType();
		}
		return rowType;
	}

	private void parseRowType() {
		// parse fields
		JsonNode fieldsNode = JsonSchemaUtils.getField(rootNode, "fields");
		for(JsonNode field : fieldsNode) {
			String fieldType = JsonSchemaUtils.getStringField(rootNode, "type");
			JsonSchemaUtils.parseFieldType(fieldType);
			System.err.println("field "+field);
		}
	}

	@Override
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}

}
