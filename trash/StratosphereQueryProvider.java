package eu.stratosphere.sql;

import java.lang.reflect.Type;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.Expression;

public class StratosphereQueryProvider implements QueryProvider {

	@Override
	public <T> Queryable<T> createQuery(Expression expression, Class<T> rowType) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

	@Override
	public <T> Queryable<T> createQuery(Expression expression, Type rowType) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

	@Override
	public <T> T execute(Expression expression, Class<T> type) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

	@Override
	public <T> T execute(Expression expression, Type type) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

	@Override
	public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
		// TODO Auto-generated method stub
		throw new RuntimeException();
	}

}
