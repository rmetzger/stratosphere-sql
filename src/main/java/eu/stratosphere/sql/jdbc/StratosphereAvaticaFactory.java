package eu.stratosphere.sql.jdbc;

import java.util.Properties;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.UnregisteredDriver;
import net.hydromatic.optiq.jdbc.OptiqJdbc41Factory;

public class StratosphereAvaticaFactory extends OptiqJdbc41Factory implements AvaticaFactory {
	@Override
	public final AvaticaConnection newConnection(UnregisteredDriver driver,
			AvaticaFactory factory, String url, Properties info) {
		// TODO Auto-generated method stub
		return super.newConnection(driver, factory, url, info);
	}
}
