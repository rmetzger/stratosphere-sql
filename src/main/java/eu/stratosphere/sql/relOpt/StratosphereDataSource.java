package eu.stratosphere.sql.relOpt;

import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;

public class StratosphereDataSource  extends TableAccessRelBase implements StratosphereRel {

	public StratosphereDataSource(
		      RelOptCluster cluster,
		      RelOptTable table) {
		    super(
		        cluster,
		        cluster.traitSetOf(Convention.NONE),
		        table);
		  }

/*	protected StratosphereDataSource(RelOptCluster cluster, RelTraitSet traits,
			RelOptTable table) {
		super(cluster, traits, table);
	} */

	@Override
	public void getStratosphereOperator() {
		
	}
	
}
