package eu.stratosphere.sql.jdbc;

import java.sql.ResultSetMetaData;
import java.util.TimeZone;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaStatement;

public class StratosphereResultSet extends AvaticaResultSet {

	public StratosphereResultSet(AvaticaStatement statement,
			AvaticaPrepareResult prepareResult,
			ResultSetMetaData resultSetMetaData, TimeZone timeZone) {
		super(statement, prepareResult, resultSetMetaData, timeZone);
	}

}
