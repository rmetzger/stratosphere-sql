package eu.stratosphere.sql;

import java.util.List;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.volcano.VolcanoPlanner;

import net.hydromatic.linq4j.expressions.ClassDeclaration;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.ConnectionConfig;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.jdbc.OptiqPrepare.SparkHandler;
import net.hydromatic.optiq.runtime.Bindable;


public class StratosphereContext implements OptiqPrepare.Context, DataContext {

    @Override
    public JavaTypeFactory getTypeFactory() {
        return new JavaTypeFactoryImpl();
    }

    @Override
    public Schema getRootSchema() {
        throw new RuntimeException();
    }

	@Override
	public ConnectionConfig config() {
		throw new RuntimeException();
	}

	@Override
	public List<String> getDefaultSchemaPath() {
		throw new RuntimeException();
	}

	@Override
	public SparkHandler spark() {
		return new SparkHandler() {
			@Override
			public Object sparkContext() {
				return null;
			}
			
			@Override
			public void registerRules(VolcanoPlanner planner) {
			}
			
			@Override
			public RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel,
					boolean restructure) {
				return null;
			}
			
			@Override
			public boolean enabled() {
				return false;
			}
			
			@Override
			public Bindable compile(ClassDeclaration expr, String s) {
				return null;
			}
		};
	}

	@Override
	public Object get(String name) {
		throw new RuntimeException();
	}

}
