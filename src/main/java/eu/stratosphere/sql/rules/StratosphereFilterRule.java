package eu.stratosphere.sql.rules;


import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelTraitSet;

import eu.stratosphere.sql.relOpt.StratosphereRel;
import eu.stratosphere.sql.relOpt.StratosphereSqlFilter;


public class StratosphereFilterRule extends ConverterRule {

	public static StratosphereFilterRule INSTANCE = new StratosphereFilterRule();
	
	public StratosphereFilterRule() {
		super(FilterRel.class,
		          Convention.NONE,
		          StratosphereRel.CONVENTION,
		          "StratosphereFilterRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		System.err.println("Converting filter");
		final FilterRel filter = (FilterRel) rel;
		final RelTraitSet traits = filter.getTraitSet().plus(StratosphereRel.CONVENTION);
		
		return new StratosphereSqlFilter(filter.getCluster(), traits, convert(filter.getChild(), traits) , filter.getCondition());
	}


}
