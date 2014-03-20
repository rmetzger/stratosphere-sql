package eu.stratosphere.sql;

import com.google.common.collect.ImmutableMap;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.impl.AbstractSchema;
import net.hydromatic.optiq.Table;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class AvroSchema extends AbstractSchema {

    final private File file;
    private Schema schema = null;

    public AvroSchema(
        SchemaPlus parentSchema,
        String name,
        File file) {
        super(parentSchema, name);
        //FixMe: only considering 1 Avro file, Fix for a directory group.;
        if(!file.exists()) {
            throw new RuntimeException("Schema repository directory "+file.getAbsolutePath()+" does not exist");
        }
        this.file = file;

    }

    //Gets schema from an AVRO file.
    public void getSchema() throws IOException {
        DatumReader<GenericRecord> datumReader =  new GenericDatumReader<GenericRecord>();
        DataFileReader reader = new DataFileReader(this.file, datumReader);
        this.schema = reader.getSchema();
    }

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        Table table = new StratosphereTable();
        String tableName = schema.getName();
        parseAvroSchema(table);
        builder.put(tableName, table);
        return builder.build();
    }

    public void parseAvroSchema(Table table) {
        List<Schema.Field> fields = schema.getFields();
        List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<Map.Entry<String, RelDataType>>();
        if (table instanceof StratosphereTable){
            RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl() ;
            for (Schema.Field field : fields ) {
                Schema.Type type = field.schema().getType();
                Map.Entry<String, RelDataType> mapPair = null;
                switch (type) {
                    case INT :
                        mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.INTEGER));
                        break;
                    case BOOLEAN :
                        mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.BOOLEAN));
                        break;
                    case BYTES  :
                        mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.TINYINT));
                        break;
                    case DOUBLE  :
                        mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.DOUBLE));
                        break;

                    case FLOAT  :
                        mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.FLOAT));
                        break;
                    case LONG  :
                        mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.BIGINT));
                        break;
                    case NULL :
                        mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.NULL));
                        break;
                    case RECORD  :
                    case STRING  :
                        mapPair = Pair.of(field.name(),typeFactory.createSqlType(SqlTypeName.CHAR));
                        break;
                    case FIXED  :
                    case UNION  :
                    case ARRAY :
                    case ENUM  :
                    case MAP  :
                    default :
                        throw new RuntimeException("Complex Avro Data Types are not supported\n");
                } //end switch
                fieldList.add(mapPair);
            } //end for
            ((StratosphereTable)table).setRowType(typeFactory.createStructType(fieldList));
        }
        //Treat exception cases
        if(fieldList.isEmpty()){
            System.err.println("ERROR: No fields defined for this table");
        }
    }

}
