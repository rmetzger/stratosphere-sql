package eu.stratosphere.sql.rules;

import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelTraitSet;

import eu.stratosphere.sql.relOpt.StratosphereRel;
import eu.stratosphere.sql.relOpt.StratosphereSqlProjection;


public class StratosphereProjectionRule extends ConverterRule {

	final public static StratosphereProjectionRule INSTANCE = new StratosphereProjectionRule();
	
	/**
	 *	public ConverterRule(
		Class<? extends RelNode> clazz,
		RelTrait in,
		RelTrait out,
		String description) {
	 */
	public StratosphereProjectionRule() {
		super(ProjectRel.class,
			Convention.NONE,
			StratosphereRel.CONVENTION,
			"StratosphereProjectionRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		System.err.println("Converting projection");
		final ProjectRel project = (ProjectRel) rel;
		final RelTraitSet traits = project.getTraitSet().plus(StratosphereRel.CONVENTION);
		return new StratosphereSqlProjection(project.getCluster(), traits, convert(project.getChild(), project.getChild().getTraitSet().plus(StratosphereRel.CONVENTION)), project.getProjects(), project.getRowType(), 0);
	}

}
