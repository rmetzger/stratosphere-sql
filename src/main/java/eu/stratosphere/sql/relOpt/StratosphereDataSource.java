package eu.stratosphere.sql.relOpt;

import java.util.List;
import java.util.Map;

import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;

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
		if (this instanceof CSVStratosphereDataSource){
			return ((CSVStratosphereDataSource)this).getStratosphereOperator(); 
		}
		else {
			System.err.println("file format not yet supported");
			return null;
		}
	}
	
}
