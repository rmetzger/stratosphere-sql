package eu.stratosphere.sql.relOpt;

import java.util.Map;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

public class FakeItDataContext implements DataContext {

	Map<String, Object> map;
	public FakeItDataContext(Map<String, Object> map) {
		this.map = map;
	}
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
		return map.get(name);
	}
}

