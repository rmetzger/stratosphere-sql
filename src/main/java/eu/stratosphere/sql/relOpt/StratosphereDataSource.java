package eu.stratosphere.sql.relOpt;

import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;

import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;

public class StratosphereDataSource  extends TableAccessRelBase implements StratosphereRel {

	public StratosphereDataSource(
		      RelOptCluster cluster,
		      RelOptTable table) {
		    super(
		        cluster,
		        cluster.traitSetOf(StratosphereRel.CONVENTION),
		        table);
		  }

/*	protected StratosphereDataSource(RelOptCluster cluster, RelTraitSet traits,
			RelOptTable table) {
		super(cluster, traits, table);
	} */

	@Override
	public Operator getStratosphereOperator() {
		FileDataSource src = new FileDataSource(new CsvInputFormat(IntValue.class, StringValue.class), "file:///home/camelia2/stratosphere_sql/stratosphere-sql-1/simple.csv");
		return src;
	}
	
}
