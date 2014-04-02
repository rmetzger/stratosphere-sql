package eu.stratosphere.sql.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;

import org.codehaus.jackson.JsonNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.util.Pair;

import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvInputFormat.ConfigBuilder;
import eu.stratosphere.sql.relOpt.StratosphereDataSource;
import eu.stratosphere.sql.relOpt.StratosphereRelUtils;
import eu.stratosphere.sql.schema.JsonSchemaUtils.FieldType;

public class CSVStratosphereTable extends AbstractStratosphereTable {

	private RelDataType rowType = null;
	FileDataSource fileSrcOperator;
	private ConfigBuilder builder;
	private JsonNode rootNode;

	public CSVStratosphereTable(JsonNode rootNode, String name) throws SchemaAdapterException {
		this.rootNode = rootNode;
		// parse "metadata"
		String path = JsonSchemaUtils.getStringField(rootNode, "filePath");
		path = JsonSchemaUtils.replaceFilenameVariables(path);
		fileSrcOperator = new FileDataSource(new CsvInputFormat(), path, "CsvInput: "+name);
		builder = CsvInputFormat.configureRecordFormat(fileSrcOperator);

		String recordDelimiter = JsonSchemaUtils.getOptionalString(rootNode, "rowDelimiter", String.valueOf('\n'));
		builder.recordDelimiter(recordDelimiter);

		char fieldDelimiter = JsonSchemaUtils.getOptionalChar(rootNode, "fieldDelimiter", ',');
		builder.fieldDelimiter(fieldDelimiter);


	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		return new StratosphereDataSource(context.getCluster(), relOptTable, fileSrcOperator);
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if(rowType == null) {
			parseRowType(typeFactory);
		}
		return rowType;
	}

	private void parseRowType(RelDataTypeFactory typeFactory) {
		// parse fields
		JsonNode fieldsNode = JsonSchemaUtils.getField(rootNode, "fields");
		int pos = 0;
		List<Map.Entry<String, RelDataType>> optiqFields = new ArrayList<Map.Entry<String,RelDataType>>();
		for(JsonNode field : fieldsNode) {
			String fieldType = JsonSchemaUtils.getStringField(field, "type");
			String fieldName = JsonSchemaUtils.getStringField(field, "name");
			FieldType typeinfo = JsonSchemaUtils.parseFieldType(fieldType);
			Pair<String, RelDataType> optiqField = null;
			if(typeinfo.arg1 != Integer.MIN_VALUE && typeinfo.arg2 != Integer.MIN_VALUE){
				optiqField = Pair.of(fieldName, typeFactory.createSqlType(typeinfo.name, typeinfo.arg1, typeinfo.arg2));
			} else if(typeinfo.arg1 != Integer.MIN_VALUE) {
				optiqField = Pair.of(fieldName, typeFactory.createSqlType(typeinfo.name, typeinfo.arg1));
			} else {
				optiqField = Pair.of(fieldName, typeFactory.createSqlType(typeinfo.name));
			}
			builder.field(StratosphereRelUtils.getTypeClass(optiqField.right), pos);
			optiqFields.add(optiqField);
			pos++;
		}
		rowType = typeFactory.createStructType(optiqFields);
		System.err.println("Created type "+rowType);
	}

	@Override
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}

}

//if(typeinfo.name.allowsPrecScale(true, false)) {
//// types with one (optional) precision argument
//if(typeinfo.arg1 != Integer.MIN_VALUE) {
//	optiqField = Pair.of(fieldName, typeFactory.createSqlType(typeinfo.name, typeinfo.arg1));
//} else {
//	optiqField = Pair.of(fieldName, typeFactory.createSqlType(typeinfo.name));
//}
//optiqFields.add(optiqField);
//} else if(typeinfo.name.allowsPrecScale(true, true)) {
//// types with both (optional) scale and precision
//}

