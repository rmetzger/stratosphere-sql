package eu.stratosphere.sql.schema;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;

import org.codehaus.jackson.JsonNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Pair;

import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvInputFormat.ConfigBuilder;
import eu.stratosphere.sql.relOpt.StratosphereDataSource;
import eu.stratosphere.sql.relOpt.StratosphereRelUtils;
import eu.stratosphere.sql.schema.JsonSchemaUtils.FieldType;
import eu.stratosphere.types.Value;
import eu.stratosphere.types.parser.DateParser;
import eu.stratosphere.types.parser.DecimalTextBigDecimalParser;
import eu.stratosphere.types.parser.FieldParser;

public class CSVStratosphereTable extends AbstractStratosphereTable {

	private RelDataType rowType = null;
	FileDataSource fileSrcOperator;
	private JsonNode rootNode;
	private CsvInputFormat format = new CsvInputFormat();

	public CSVStratosphereTable(JsonNode rootNode, String name) throws SchemaAdapterException {
		this.rootNode = rootNode;
		// parse "metadata"
		String path = JsonSchemaUtils.getStringField(rootNode, "filePath");
		path = JsonSchemaUtils.replaceFilenameVariables(path);
		fileSrcOperator = new FileDataSource(format, path, "CsvInput: "+name);

		String recordDelimiter = JsonSchemaUtils.getOptionalString(rootNode, "rowDelimiter", String.valueOf('\n'));
		format.setDelimiter(recordDelimiter);

		char fieldDelimiter = JsonSchemaUtils.getOptionalChar(rootNode, "fieldDelimiter", ',');
		format.setFieldDelimiter(fieldDelimiter);
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
		Class<? extends Value>[] formatFields = new Class[fieldsNode.size()];
		//String[] dateFormatStrings = new String[fieldsNode.size()]; // this is a waste but I'm lazy
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
			formatFields[pos] = StratosphereRelUtils.getTypeClass(optiqField.right);
			optiqFields.add(optiqField);
			pos++;
		}
		// create row type for optiq
		rowType = typeFactory.createStructType(optiqFields);
		// configure csv input format
		format.setFieldTypesArray(formatFields);
		// assign parsers within the input format.
		int i = 0;
		for(Entry<String, RelDataType> field : optiqFields) {
			RelDataType relType = field.getValue();
			if(relType.getSqlTypeName() == SqlTypeName.DECIMAL) {
				FieldParser<?> parser = format.getFieldParser(i);
				((DecimalTextBigDecimalParser) parser).enforceScale(relType.getScale(), RoundingMode.HALF_UP);
			}
			if(relType.getSqlTypeName() == SqlTypeName.DATE) {
				JsonNode dateFormatNode = fieldsNode.get(i).get("format");
				if(dateFormatNode != null) {
					FieldParser<?> parser = format.getFieldParser(i);
					((DateParser) parser).setFormat(dateFormatNode.asText());
				}
			}
			i++;
		}


		System.err.println("Created type "+rowType);
	}

	@Override
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}

}
