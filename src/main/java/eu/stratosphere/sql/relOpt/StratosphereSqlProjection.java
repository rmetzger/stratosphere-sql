package eu.stratosphere.sql.relOpt;

import java.util.List;

import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

public class StratosphereSqlProjection extends ProjectRelBase implements StratosphereRel {

	
	public StratosphereSqlProjection(RelOptCluster cluster,
			RelTraitSet traits, RelNode child, List<RexNode> exps,
			RelDataType rowType, int flags) {
		super(cluster, traits, child, exps, rowType, flags);
	}

	@Override
	public void getStratosphereOperator() {
		// TODO Auto-generated method stub
		
	}

}
