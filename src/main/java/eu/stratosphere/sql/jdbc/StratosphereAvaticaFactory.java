package eu.stratosphere.sql.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaDatabaseMetaData;
import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaPreparedStatement;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaResultSetMetaData;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.Meta;
import net.hydromatic.avatica.UnregisteredDriver;

public class StratosphereAvaticaFactory implements AvaticaFactory {

	@Override
	public int getJdbcMajorVersion() {
		return 4;
	}

	@Override
	public int getJdbcMinorVersion() {
		return 0;
	}

	@Override
	public AvaticaConnection newConnection(UnregisteredDriver driver,
			AvaticaFactory factory, String url, Properties info)
			throws SQLException {
		return new StratosphereConnection(driver, factory, url, info);
	}

	@Override
	public AvaticaStatement newStatement(AvaticaConnection connection,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return new StratosphereStatement( (StratosphereConnection) connection, 
				resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public AvaticaPreparedStatement newPreparedStatement(
			AvaticaConnection connection, AvaticaPrepareResult prepareResult,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new RuntimeException("TODO");
	}

	@Override
	public AvaticaResultSet newResultSet(AvaticaStatement statement,
			AvaticaPrepareResult prepareResult, TimeZone timeZone)
			throws SQLException {
		final ResultSetMetaData metaData = newResultSetMetaData(statement, prepareResult.getColumnList());
		return new StratosphereResultSet(statement, prepareResult, metaData, timeZone);
	}

	@Override
	public AvaticaDatabaseMetaData newDatabaseMetaData(
			AvaticaConnection connection) {
		return new StratosphereDatabaseMetaData( (StratosphereConnection) connection);
	}

	@Override
	public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
			List<ColumnMetaData> columnMetaDataList) throws SQLException {
		return new AvaticaResultSetMetaData(statement, null, columnMetaDataList);
	}

	public static class StratosphereConnection extends AvaticaConnection {

		protected StratosphereConnection(UnregisteredDriver driver,
				AvaticaFactory factory, String url, Properties info) {
			super(driver, factory, url, info);
		}
		
		@Override
		protected Meta createMeta() {
			System.err.println("createMeta()");
			return new StratosphereMeta();
		}
	}

	private static class StratosphereDatabaseMetaData extends
			AvaticaDatabaseMetaData {
		StratosphereDatabaseMetaData(StratosphereConnection connection) {
			super(connection);
		}
	}

	public class StratosphereStatement extends AvaticaStatement {

		StratosphereStatement(StratosphereConnection connection,
				int resultSetType, int resultSetConcurrency,
				int resultSetHoldability) {
			super(connection, resultSetType, resultSetConcurrency,
					resultSetHoldability);
		}

		@Override
		public StratosphereConnection getConnection() {
			return (StratosphereConnection) connection;
		}

	}
}

