/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.sql.schema;

import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.io.avro.AvroRecordInputFormat;
import eu.stratosphere.sql.relOpt.StratosphereDataSource;
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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class AvroStratosphereTable extends AbstractStratosphereTable {

	private RelDataType rowType = null;
	FileDataSource fileSrcOperator;

	public AvroStratosphereTable(JsonNode rootNode, String name) throws SchemaAdapterException {
		// parse "metadata"
		String path = JsonSchemaUtils.getStringField(rootNode, "filePath");
		path = JsonSchemaUtils.replaceFilenameVariables(path);
		fileSrcOperator = new FileDataSource(new AvroRecordInputFormat(), path, "AvroInput: "+name);
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		return new StratosphereDataSource(context.getCluster(), relOptTable, fileSrcOperator);
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory)  {
		if(rowType == null) {
			parseRowType(typeFactory);
		}
		return rowType;
	}

    private Schema getSchema()  {
        DatumReader<GenericRecord> datumReader =  new GenericDatumReader<GenericRecord>();
        try {
            DataFileReader reader = new DataFileReader(new File(fileSrcOperator.getFilePath()), datumReader);
            return reader.getSchema();
        } catch (IOException e) {
            throw new RuntimeException("Error while accessing schema from Avro file");
        }
    }

	private void parseRowType(RelDataTypeFactory typeFactory) {
		// parse fields
		List<Entry<String, RelDataType>> optiqFields = new ArrayList<Entry<String,RelDataType>>();
        Schema schema = getSchema();

        for (Schema.Field field : schema.getFields() ) {
            Schema.Type type = field.schema().getType();
            Map.Entry<String, RelDataType> mapPair = null;
            switch (type) {
                case INT :
                    mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.INTEGER));
                    break;
                case BOOLEAN :
                    mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.BOOLEAN));
                    break;
                case BYTES :
                    mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.TINYINT));
                    break;
                case DOUBLE :
                    mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.DOUBLE));
                    break;
                case FLOAT :
                    mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.FLOAT));
                    break;
                case LONG :
                    mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.BIGINT));
                    break;
                case NULL :
                    mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.NULL));
                    break;
                case RECORD :
                case STRING :
                    mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.CHAR));
                    break;
                case FIXED :
                case UNION :
                case ARRAY :
                case ENUM :
                case MAP :
                default :
                    throw new RuntimeException("Complex Avro Data Types are not supported\n");
            } //end switch
            optiqFields.add(mapPair);
        }//end for

		// create row type for optiq
		rowType = typeFactory.createStructType(optiqFields);

		System.err.println("Created type "+rowType);
	}

	@Override
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}

}
