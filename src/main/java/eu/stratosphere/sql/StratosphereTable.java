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

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonToken;


public class StratosphereTable implements TranslatableTable {

	private RelDataType rowType;
	public String primaryKey;
	public String filePath;
	public String columnDelimiter;
	public String rowDelimiter;
	
	private static ObjectMapper mapper = new ObjectMapper();
	private static JsonFactory factory = mapper.getJsonFactory();

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
		    	parser = factory.createJsonParser(reader);;  
		    	System.err.println("create parser ok");
		    	JsonToken token = null;
			    while ((token = parser.nextToken()) != null) {
			    	System.err.println("token: " + token.toString()+ " " + parser.getText());
			      
			    	if (token == JsonToken.FIELD_NAME) {
				        if(parser.getText().equals("fields")) {			                
			                	JsonToken token2 = parser.nextToken();
			                    
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
			                    token2 = parser.nextToken();   
			                    while(token2 != JsonToken.END_ARRAY){
			                    	 fieldName = null;
			                    	 fieldType = null;
			                    	 Map.Entry<String, RelDataType> field = null;
			                    	 token2 = parser.nextToken(); 			                    	 
			                    	 if((token2 == JsonToken.FIELD_NAME) && (parser.getText().toLowerCase().equals("name"))) {
			                    		 token2 = parser.nextToken(); 
			                    		 fieldName = parser.getText();
			                    		 System.err.println("one column: " + fieldName);
			                    	 }
			                    	 token2 = parser.nextToken(); 
			                    	 if((token2 == JsonToken.FIELD_NAME) && (parser.getText().toLowerCase().equals("type"))) {
			                    		 token2 = parser.nextToken(); 
			                    		 fieldType = parser.getText();
			                    		 System.err.println(" of type: " + fieldType);
			                    		 switch (fieldType.toUpperCase()){
			                    		 	case "INTEGER":
			                    		 		field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.INTEGER));
			                    		 		break;
			                    		 	case "VARCHAR":
			                    		 		field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.VARCHAR));
			                    		 		break;
			                    		 	case "CHAR":
			                    		 		field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.CHAR));
			                    		 		break;
			                    		 }
			                    		 fieldList.add(field);
			                    	 }			                    	 
			                    	 token2 = parser.nextToken(); 
			                    	 token2 = parser.nextToken(); 			                    	 
			                    }
			                    
				        	}
				        else if (parser.getText().equals("primaryKey")){
			                	 token = parser.nextToken();
			                     primaryKey = parser.getText();
			                     System.err.println("primaryKey: " + primaryKey);			                     
				        	}
				        	else if (parser.getText().equals("columnDelimiter")){
				                	 token = parser.nextToken();
				                     columnDelimiter = parser.getText();
				                     System.err.println("columnDelimiter: " + columnDelimiter);			                     
				        		}
				        		else if (parser.getText().equals("rowDelimiter")){
					                	 token = parser.nextToken();
					                     rowDelimiter = parser.getText();			                     			                     
				        			}
				        			else if (parser.getText().equals("filePath")){
						                	 token = parser.nextToken();
						                     filePath = parser.getText();
						                     System.err.println("filePath:" + filePath);
				        			}
			      }
			      else {
			    	  System.err.println("token:" + token.toString());
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
