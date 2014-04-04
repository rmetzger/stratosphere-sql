package eu.stratosphere.sql.jdbc;

import java.util.List;

import net.hydromatic.avatica.AvaticaParameter;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.ColumnMetaData;

public class StratospherePrepareResult implements AvaticaPrepareResult {

	@Override
	public List<ColumnMetaData> getColumnList() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSql() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<AvaticaParameter> getParameterList() {
		// TODO Auto-generated method stub
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
