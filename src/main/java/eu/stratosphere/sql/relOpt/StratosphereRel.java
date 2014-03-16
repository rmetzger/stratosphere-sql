package eu.stratosphere.sql.relOpt;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

import eu.stratosphere.api.common.operators.Operator;

public interface StratosphereRel extends RelNode {
	
	Convention CONVENTION = new Convention.Impl("STRATOSPHERE", StratosphereRel.class);
	
	/**
	 * Returns 
	 */
	public Operator getStratosphereOperator();
	
}
