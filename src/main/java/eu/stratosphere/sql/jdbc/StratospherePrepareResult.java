package eu.stratosphere.sql.jdbc;

import java.sql.ResultSetMetaData;
import java.util.List;

import net.hydromatic.avatica.AvaticaParameter;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.ColumnMetaData.Rep;

import com.google.common.collect.ImmutableList;

public class StratospherePrepareResult implements AvaticaPrepareResult {
	private String sql;
	public StratospherePrepareResult(AvaticaStatement statement, String sql) {
		this.sql = sql;
	}

	@Override
	public List<ColumnMetaData> getColumnList() {
		System.err.println("GetColList");
		int i = 1;
		return ImmutableList.of( new ColumnMetaData( //
		          i, // ordinal
		          false, // autoIncrement
		          true, // caseSensitive
		          false, // searchable
		          false, // currency
		          ResultSetMetaData.columnNoNulls, //nullability 
		          false, // signed
		          10, // display size.
		          "test1", // label
		          "test2", // columnname
		          "test3", // schemaname
		          0, // precision
		          0, // scale
		          "test3.1", // tablename is null so sqlline doesn't try to retrieve primary keys.
		          "test4", // catalogname
		          Rep.LONG.ordinal(),  // sql type
		          "test5", // typename
		          true, // readonly
		          false, // writable
		          false, // definitely writable
		          "none", // column class name
		          ColumnMetaData.Rep.BOOLEAN // Dummy value for representation as it doesn't apply in drill's case.
		          ));
	}

	@Override
	public String getSql() {
		return sql;
	}

	@Override
	public List<AvaticaParameter> getParameterList() {
		System.err.println("getParameterList");
		return null;
	}
	

//	final String sql;
//	  final DrillColumnMetaDataList columns = new DrillColumnMetaDataList();
//	  
//	  public DrillPrepareResult(String sql) {
//	    super();
//	    this.sql = sql;
//	  }
//
//	  @Override
//	  public List<ColumnMetaData> getColumnList() {
//	    return columns;
//	  }
//
//	  @Override
//	  public String getSql() {
//	    return sql;
//	  }
//
//	  @Override
//	  public List<AvaticaParameter> getParameterList() {
//	    return Collections.emptyList();
//	  }

}
