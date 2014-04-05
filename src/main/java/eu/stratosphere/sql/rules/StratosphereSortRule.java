package eu.stratosphere.sql.rules;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelTraitSet;

import eu.stratosphere.sql.relOpt.StratosphereRel;
import eu.stratosphere.sql.relOpt.StratosphereSqlSort;


public class StratosphereSortRule extends ConverterRule {

	final public static StratosphereSortRule INSTANCE = new StratosphereSortRule();

	public StratosphereSortRule() {
		super(SortRel.class,
			Convention.NONE,
			StratosphereRel.CONVENTION,
			"StratosphereSortRule");
	}
	
//	// stolen from drill
//	@Override
//	public boolean matches(RelOptRuleCall call) {
//		final SortRel sort = call.rel(0);
//		return sort.offset == null && sort.fetch == null;
//	}

	@Override
	public RelNode convert(RelNode rel) {
		System.err.println("Converting sort: "+rel);
		final SortRel sort = (SortRel) rel;
		final RelTraitSet sortTraits = sort.getTraitSet().plus(StratosphereRel.CONVENTION);
		final RelTraitSet childTraits = sort.getChild().getTraitSet().plus(StratosphereRel.CONVENTION);
		return new StratosphereSqlSort(sort.getCluster(), sortTraits, convert(sort.getChild(), childTraits), 
				sort.getCollation(), sort.offset, sort.fetch);
	}

}
