package eu.stratosphere.sql.relOpt;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.reltype.RelDataType;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.types.Value;

public interface StratosphereRel extends RelNode {
	
	Convention CONVENTION = new Convention.Impl("STRATOSPHERE", StratosphereRel.class);
	
	/**
	 * Returns 
	 */
	public Operator getStratosphereOperator();
	
}
