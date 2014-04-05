package eu.stratosphere.sql.relOpt;

import java.util.Iterator;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * The whole sort support here is really poor. I would not recommend to use this in
 * production except for very small stuff to sort (since its actually not parallel)
 *
 */
public class StratosphereSqlSort extends SortRel implements StratosphereRel {

	public StratosphereSqlSort(RelOptCluster cluster, RelTraitSet traits,
			RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
		super(cluster, traits, child, collation, offset, fetch);
		System.err.println("I have a sort here with convention "+getConvention());
		Preconditions.checkArgument(getConvention() == CONVENTION);
	}

	@Override
	public SortRel copy(RelTraitSet traitSet, RelNode newInput,
			RelCollation newCollation, RexNode offset, RexNode fetch) {
		return new StratosphereSqlSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
	}
	
	
	public static class SortUdf extends ReduceFunction {

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out)
				throws Exception {
			while(records.hasNext()) {
				out.collect(records.next());
			}
		}
	}
	
	@Override
	public Operator getStratosphereOperator() {
		final Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());
		ReduceOperator.Builder sortBuilder = ReduceOperator.builder(new SortUdf());
		
		sortBuilder.input(inputOp);
		Ordering order = new Ordering();
		RelCollation coll = getCollation();
		for(RelFieldCollation fieldColl : coll.getFieldCollations()) {
			Class<? extends Key> keyClass = StratosphereRelUtils.getKeyTypeClass(getInput(0).getRowType().getFieldList().get(fieldColl.getFieldIndex()).getType());
			sortBuilder.keyField(keyClass, fieldColl.getFieldIndex());
			order.appendOrdering(fieldColl.getFieldIndex(), keyClass, Order.valueOf(fieldColl.direction.name()));
		}
		System.err.println("Setting order to "+order);
		sortBuilder.secondaryOrder(order);
		Operator sortOp =  sortBuilder.build();
		sortOp.setDegreeOfParallelism(1);
		return sortOp;
	}

}
