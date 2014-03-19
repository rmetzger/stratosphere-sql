package eu.stratosphere.sql.jdbc;

import net.hydromatic.avatica.DriverVersion;

public class StratosphereDriverVersion extends DriverVersion {

	public StratosphereDriverVersion() {
		super("Stratosphere SQL JDBC Driver", 
				"1.0", 
				"Stratosphere SQL", 
				"0.5-SNAPSHOT", 
				true,
				1, 
				0, 
				1, 
				0);
	}

}
