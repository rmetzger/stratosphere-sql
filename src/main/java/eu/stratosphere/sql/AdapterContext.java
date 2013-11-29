package eu.stratosphere.sql;

import java.util.List;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.ConnectionConfig;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.jdbc.OptiqPrepare.SparkHandler;


public class AdapterContext implements OptiqPrepare.Context {

    @Override
    public JavaTypeFactory getTypeFactory() {
        // adapter implementation
        return null;
    }

    @Override
    public Schema getRootSchema() {
        // adapter implementation
        return null;
    }

	@Override
	public ConnectionConfig config() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getDefaultSchemaPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SparkHandler spark() {
		// TODO Auto-generated method stub
		return null;
	}

}
