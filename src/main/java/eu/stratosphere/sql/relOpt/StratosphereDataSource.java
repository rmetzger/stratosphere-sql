package eu.stratosphere.sql.relOpt;

import java.util.List;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.Operator;

public class StratosphereDataSource	extends TableAccessRelBase implements StratosphereRel {

	private Operator op;
	public StratosphereDataSource(RelOptCluster cluster,RelOptTable table, Operator op) {
		super(cluster,cluster.traitSetOf(StratosphereRel.CONVENTION), table);
		Preconditions.checkArgument(getConvention() == CONVENTION);
		Preconditions.checkNotNull(op);
		this.op = op;
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new StratosphereDataSource(getCluster(), getTable(), getStratosphereOperator());
	}

	@Override
	public Operator getStratosphereOperator() {
		return op;
	}
}
