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

import eu.stratosphere.sql.relOpt.StratosphereDataSource;
import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;

import javax.json.Json;
import javax.json.stream.JsonParser;


public class StratosphereTable implements TranslatableTable {

	private RelDataType rowType;
	private String primaryKey;
	private String filePath;
	private String columnDelimiter;

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if(rowType == null) {
			List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<Map.Entry<String, RelDataType>>();
			
			/*  was before:
			 * 
			 *  Map.Entry<String, RelDataType> first = Pair.of("customerId", typeFactory.createSqlType(SqlTypeName.INTEGER));
				Map.Entry<String, RelDataType> second = Pair.of("customerName", typeFactory.createSqlType(SqlTypeName.VARCHAR));
				fieldList.add(first);
				fieldList.add(second);
				rowType = typeFactory.createStructType(fieldList);
						//typeFactory.createSqlType(SqlTypeName.ROW);
			* 
			*/
			
			//read the table structure from a JSON file
			JsonParser parser = null;
		    FileReader reader = null;
		    File file = null;
		    try {
		    	file = new File ("/home/camelia2/stratosphere_sql/stratosphere-sql-1/simple.json");
		    	reader = new FileReader(file);
		    	System.err.println("json file found");
		    	parser = Json.createParser(reader);  
		    	System.err.println("create parser ok");
			    while (parser.hasNext()) {
			      JsonParser.Event event = parser.next();
			      if (event == JsonParser.Event.KEY_NAME) {
			        switch (parser.getString()) {
			                 case "fields":
			                	JsonParser.Event event2 = parser.next();
			                    System.err.println("fields: " + event.toString());
			                    /* event2 helps in iterating in the fields array
			                     * 
			                     * event2: START_ARRAY            --> the start of the array
			                     * 
			                     * event2: START_OBJECT           --> one for each field
			                     * event2: KEY_NAME               --> "name" token
			                     * event2: VALUE_STRING           -->  the field's name
			                     * event2: KEY_NAME               --> "type" token
			                     * event2: VALUE_STRING           -->  the field's type
			                     * event2: END_OBJECT             --> end of one field
			                     * ...
			                     * 
			                     * breaking the iteration when event2 becomes END_ARRAY
			                     */
			                    String fieldName;
			                    String fieldType;
			                    event2 = parser.next();   
			                    while(event2 != JsonParser.Event.END_ARRAY){
			                    	 fieldName = null;
			                    	 fieldType = null;
			                    	 Map.Entry<String, RelDataType> field = null;
			                    	 event2 = parser.next(); 			                    	 
			                    	 if((event2 == JsonParser.Event.KEY_NAME) && (parser.getString().toLowerCase().equals("name"))) {
			                    		 event2 = parser.next();
			                    		 fieldName = parser.getString();
			                    		 System.err.println("one column: " + fieldName);
			                    	 }
			                    	 event2 = parser.next(); 	
			                    	 if((event2 == JsonParser.Event.KEY_NAME) && (parser.getString().toLowerCase().equals("type"))) {
			                    		 event2 = parser.next();
			                    		 fieldType = parser.getString();
			                    		 System.err.println(" of type: " + fieldType);
			                    		 switch (fieldType){
			                    		 	case "int":
			                    		 		field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.INTEGER));
			                    		 		break;
			                    		 	case "string":
			                    		 		field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.VARCHAR));
			                    		 		break;
			                    		 }
			                    		 fieldList.add(field);
			                    	 }			                    	 
			                    	 event2 = parser.next(); 	
			                    	 event2 = parser.next(); 	
			                    	 
			                    }
			                    
			                    break;
			                 case "primaryKey":
			                     parser.next();
			                     primaryKey = parser.getString();
			                     System.err.println("primaryKey: " + primaryKey);
			                     
			                     break;
			                 case "columnDelimiter":
			                     parser.next();
			                     columnDelimiter = parser.getString();
			                     System.err.println("columnDelimiter: " + columnDelimiter);
			                     
			                     break;
			                 case "filePath":
			                     parser.next();
			                     filePath = parser.getString();
			                     System.err.println("filePath:" + filePath);
			                     
			                     break;                	 
			        }//switch
			      }
			      else {
			    	  System.err.println("event:" + event.toString());
			      }
			      
			    }//while
			    
			    rowType = typeFactory.createStructType(fieldList);

		    } catch (IOException e) {
		        // ignore
		      } finally {
		        if (reader != null) {
		          try {
		            reader.close();
		          } catch (IOException e) {
		            // ignore
		          }
		        }
		      }
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
