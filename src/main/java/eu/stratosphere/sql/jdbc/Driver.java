package eu.stratosphere.sql.jdbc;

import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.DriverVersion;
import net.hydromatic.avatica.UnregisteredDriver;

public class Driver extends UnregisteredDriver{
	public static final String CONNECT_STRING_PREFIX = "jdbc:stratosphere:";
	
	static {
		new Driver().register();
	}
	
	
	@Override
	protected DriverVersion createDriverVersion() {
		return new StratosphereDriverVersion();
	}

	@Override
	protected String getConnectStringPrefix() {
		return CONNECT_STRING_PREFIX;
	}
	

//	@Override
//	protected String getFactoryClassName(JdbcVersion jdbcVersion) {
//		switch (jdbcVersion) {
////		case JDBC_30:
////			return "org.apache.drill.jdbc.DrillJdbc3Factory";
////		case JDBC_40:
////			return "org.apache.drill.jdbc.DrillJdbc40Factory";
//		case JDBC_41:
//		default:
//			return "net.hydromatic.optiq.jdbc.OptiqJdbc41Factory";
//		}
//	}
	
//	@Override
//	protected AvaticaFactory createFactory() {
//		System.err.println("Create factory");
//		return new StratosphereAvaticaFactory();
//	}
}
