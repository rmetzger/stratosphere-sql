package eu.stratosphere.sql.relOpt;

import java.util.Arrays;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

public class StratosphereDataContext implements DataContext {
	private static final int MAX_CONTEXT_SIZE = 1024;
	public static final String REC_FIELD = "inputRecord";
	// enforce ArrayList for faster access (compared to LL)
	Object[] elements;
	public StratosphereDataContext() {
		this.elements = new Object[1];
	}

	@Override
	public SchemaPlus getRootSchema() {
		throw new RuntimeException("unsupported");
	}

	@Override
	public JavaTypeFactory getTypeFactory() {
		throw new RuntimeException("unsupported");
	}

	@Override
	public QueryProvider getQueryProvider() {
		throw new RuntimeException("unsupported");
	}

	@Override
	public Object get(String name) {
		if(name.equals(REC_FIELD)) {
			return elements;
		} else {
			throw new RuntimeException("Unknown element "+name);
		}
	}

	public Object get(int i) {
		if(i < 0) {
			throw new IndexOutOfBoundsException("Accessing index smaller zero");
		}
		if(i > elements.length-1) {
			throw new IndexOutOfBoundsException("Accessing index greater than array");
		}
		return elements[i];
	}

	public void set(int i, Object v) {
		if(i < 0) {
			throw new IndexOutOfBoundsException("Accessing index smaller zero");
		}
		if(i > elements.length-1) {
			// need to grow
			do {
				if(elements.length * 2 > MAX_CONTEXT_SIZE) {
					throw new RuntimeException("The internal array can not grow beyond "+MAX_CONTEXT_SIZE);
				}
				elements = Arrays.copyOf(elements, elements.length * 2);
			} while (i > elements.length-1);
		}
		elements[i] = v;
	}
}

