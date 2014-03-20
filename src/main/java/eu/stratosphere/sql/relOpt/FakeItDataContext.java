package eu.stratosphere.sql.relOpt;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

public class FakeItDataContext implements DataContext {

	@Override
	public SchemaPlus getRootSchema() {
		throw new RuntimeException("haha");
	}

	@Override
	public JavaTypeFactory getTypeFactory() {
		throw new RuntimeException("haha");
	}

	@Override
	public QueryProvider getQueryProvider() {
		throw new RuntimeException("haha");
	}

	@Override
	public Object get(String name) {
		throw new RuntimeException("haha");
	}
}

