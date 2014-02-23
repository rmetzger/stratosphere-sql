package eu.stratosphere.sql.relOpt;

import java.util.List;

import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class StratosphereSqlProjection extends ProjectRelBase implements StratosphereRel {

	/**
	 * Simply pass the record through.
	 */
	public static class StratosphereSqlProjectionMapOperator extends MapFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			out.collect(record);
		}
		
	}
	
	public StratosphereSqlProjection(RelOptCluster cluster,
			RelTraitSet traits, RelNode child, List<RexNode> exps,
			RelDataType rowType, int flags) {
		super(cluster, traits.plus(CONVENTION), child, exps, rowType, flags);
	}

	@Override
	public Operator getStratosphereOperator() {
		List<RelNode> optiqInput = getInputs();
		Operator inputOp = null;
		if(optiqInput.size() == 1) {
			RelNode optiqSingleInput = sole(optiqInput);
			if(!(optiqSingleInput instanceof StratosphereRel)) {
				throw new RuntimeException("Input not properly converted to StratosphereRel");
			}
			inputOp = ( (StratosphereRel)optiqSingleInput).getStratosphereOperator();
		} else {
			throw new RuntimeException("Multiple inputs not supported at this time");
		}
		MapOperator proj = MapOperator	.builder(new StratosphereSqlProjectionMapOperator())
										.input(inputOp)
										.name("SQL Projection")
										.build();
		return proj;
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new StratosphereSqlProjection(getCluster(), traitSet, sole(inputs), getChildExps(), getRowType(), getFlags());
	}
}
