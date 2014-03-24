/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package eu.stratosphere.sql;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.AbstractSchema;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Pair;

import com.google.common.collect.ImmutableMap;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class CsvSchema extends AbstractSchema {
	final private  File directoryFile;
	private static ObjectMapper mapper = new ObjectMapper();
	private static JsonFactory factory = mapper.getJsonFactory();


	/**
	 * Creates a CSV schema.
	 *
	 * @param parentSchema Parent schema
	 * @param name Schema name
	 * @param directoryFile Directory that holds .csv files
	 * @param smart		Whether to instantiate smart tables that undergo
	 *					 query optimization
	 */
	public CsvSchema(
		SchemaPlus parentSchema,
		String name,
		File directoryFile) {
	super(parentSchema, name);
	this.directoryFile = directoryFile;
	if(!directoryFile.exists()) {
		throw new RuntimeException("Schema repository directory "+directoryFile.getAbsolutePath()+" does not exist");
	}
	}

	@Override
	protected Map<String, Table> getTableMap() {
	final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
	System.out.println("Working Directory = " +
			System.getProperty("user.dir"));
	File[] files = directoryFile.listFiles(
		new FilenameFilter() {
			public boolean accept(File dir, String name) {
			return name.endsWith(".json");
			}
		});
	if (files == null) {
		System.out.println("directory " + directoryFile + " not found");
		files = new File[0];
	}
	for (File file : files) {
		String tableName = file.getName();
		if (tableName.endsWith(".json")) {
		tableName = tableName.substring(0, tableName.length() - ".json".length());
		System.err.println("TABLE " + tableName.toUpperCase());
		Table table = new StratosphereTable();
		this.parseJSONSchema(table, file);
		builder.put(tableName, table);

		}
	}
	return builder.build();
	}
	
	public void parseJSONSchema(Table table, File file){
		RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl() ;
		List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<Map.Entry<String, RelDataType>>();
			
		//read the table structure from a JSON file
		((StratosphereTable)table).jsonFileName = file.getAbsolutePath();
		JsonParser parser = null;
		FileReader reader = null;
		if (table instanceof StratosphereTable){
			try {
					reader = new FileReader(file);
					parser = factory.createJsonParser(reader);
					JsonToken token = null;
					while ((token = parser.nextToken()) != null) {
						
						if (token == JsonToken.FIELD_NAME) {
							if(parser.getText().equals("ignore")) {
								return; // ignore this table
							} else if(parser.getText().equals("fields")) {							
									JsonToken token2 = parser.nextToken();
									
									/* event2 helps in iterating in the fields array
									 * 
									 * event2: START_ARRAY			--> the start of the array
									 * 
									 * event2: START_OBJECT			 --> one for each field
									 * event2: KEY_NAME				 --> "name" token
									 * event2: VALUE_STRING			 -->	the field's name
									 * event2: KEY_NAME				 --> "type" token
									 * event2: VALUE_STRING			 -->	the field's type
									 * event2: END_OBJECT			 --> end of one field
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
											System.err.print("" + fieldName);
										}
										token2 = parser.nextToken(); 
										if((token2 == JsonToken.FIELD_NAME) && (parser.getText().toLowerCase().equals("type"))) {
											token2 = parser.nextToken(); 
											fieldType = parser.getText();
											System.err.println("	" + fieldType);
											if(fieldType.toUpperCase().equals("INTEGER")){	
												field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.INTEGER));
											} else
											if(fieldType.toUpperCase().equals("LONG")){	
												field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.BIGINT));
											} else
											if(fieldType.toUpperCase().equals("SHORT")){	
												field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.SMALLINT));
											}	else
											if(fieldType.toUpperCase().equals("BYTE")){	
												field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.TINYINT));
											}	else
											if(fieldType.toUpperCase().equals("FLOAT")){	
												field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.FLOAT));
											}	else
											if(fieldType.toUpperCase().equals("DOUBLE")){	
												field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.DOUBLE));
											}	else
											if(fieldType.toUpperCase().equals("CHAR")){	
												field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.CHAR));
											}	else
											if(fieldType.toUpperCase().equals("VARCHAR")){	
												field = Pair.of(fieldName, typeFactory.createSqlType(SqlTypeName.VARCHAR));
											}	
											
											fieldList.add(field);
										}
										token2 = parser.nextToken();
										token2 = parser.nextToken();
									}
									
								}else if (parser.getText().equals("primaryKey")){
									token = parser.nextToken();
									((StratosphereTable)table).primaryKey = parser.getText();							 
								} else if (parser.getText().equals("columnDelimiter")){
										token = parser.nextToken();
										((StratosphereTable)table).columnDelimiter = parser.getText();						 
									} else if (parser.getText().equals("rowDelimiter")){
											token = parser.nextToken();
											((StratosphereTable)table).rowDelimiter = parser.getText();								 								 
										} else if (parser.getText().equals("filePath")){
												token = parser.nextToken();
												((StratosphereTable)table).filePath = parser.getText();
										}
						}
						
					}//while
					
					((StratosphereTable)table).setRowType(typeFactory.createStructType(fieldList));
		
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
	
		//Treat exception cases
		if(fieldList.isEmpty()){
			System.err.println("ERROR: No fields defined for this table");
			return;
		}
		if(((StratosphereTable)table).filePath == null){
			System.err.println("ERROR: No file path specified for this table data file");
			return;			
		}
	}

	
}

// End CsvSchema.java
