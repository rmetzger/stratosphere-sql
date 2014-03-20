package eu.stratosphere.sql.relOpt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.Schemas;

import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexExecutorImpl;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.ReflectionUtil;

public class StratosphereSqlProjection extends ProjectRelBase implements StratosphereRel {

	//
	// Optiq related
	// 
	public StratosphereSqlProjection(RelOptCluster cluster,
			RelTraitSet traits, RelNode child, List<RexNode> exps,
			RelDataType rowType, int flags) {
		super(cluster, traits, child, exps, rowType, flags);
		Preconditions.checkArgument(getConvention() == CONVENTION);
	}
	
	@Override
	public ProjectRelBase copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps, RelDataType rowType) {
		return new StratosphereSqlProjection(getCluster(), traitSet, input, exps, rowType, getFlags());
	}
	

	//
	// Stratosphere related
	// 
	
	/**
	 * Simply pass the record through.
	 */
	public static class StratosphereSqlProjectionMapOperator extends MapFunction {
		private static final long serialVersionUID = 1L;
		private List<Entry<Integer, ? extends Class<? extends Value>>> types;
		private Record outRec = new Record();
		
		public StratosphereSqlProjectionMapOperator(List<Entry<Integer, ? extends Class<? extends Value>>> types) {
			this.types = types;
		}

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			outRec.clear();
			Iterator<Entry<Integer, ? extends Class<? extends Value>>> it = types.iterator();
			while(it.hasNext()) {
				Entry<Integer, ? extends Class<? extends Value>> desc = it.next();
				Value val = ReflectionUtil.newInstance(desc.getValue());
				record.getFieldInto(desc.getKey(), val);
				outRec.addField(val);
			}
			out.collect(outRec);
		}
		
	}
	
	

	@Override
	public Operator getStratosphereOperator() {
		// get Input
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());
		List<Map.Entry<Integer, ? extends Class<? extends Value>>> types = new ArrayList<Map.Entry<Integer, ? extends Class<? extends Value>>>();
		Iterator<RexNode> it = exps.iterator();
		
		while(it.hasNext()) {
			RexNode node = it.next();
			
			final RexBuilder rexBuilder = getCluster().getRexBuilder();
            DataContext dataContext = new FakeItDataContext();
            final RexExecutorImpl executor = new RexExecutorImpl(dataContext);
            List<RexNode> reducedValues = new ArrayList<RexNode>();
            List<RexNode> inputExprs = new ArrayList<RexNode>();
            inputExprs.add(node);
            RelDataType type = getCluster().getTypeFactory().createJavaType(String.class);
			rexBuilder.makeInputRef(type, 0);
//			
			executor.execute(rexBuilder, ImmutableList.<RexNode>copyOf(inputExprs),
                    reducedValues);
			
			for(RexNode r : reducedValues) {
				System.err.println("Rex node "+r);
			}
			
//			RexInputRef inputRef = (RexInputRef) it.next();
//			Pair<Integer, ? extends Class<? extends Value>> entry = new Pair(inputRef.getIndex(), StratosphereRelUtils.getTypeClass(inputRef.getType()));
//			types.add(entry);
		}
	
		// create MapOperator
		MapOperator proj = MapOperator	.builder(new StratosphereSqlProjectionMapOperator(types))
										.input(inputOp)
										.name(buildName())
										.build();
		return proj;
	}

	private String buildName() {
		return "Project "+getRowType().toString();
	}

	

	public Class<? extends Value>[] getFields() {
		Class<? extends Value>[] fields = new Class[this.exps.size()];
		Iterator<RexNode> it = exps.iterator();
		int i = 0;
		while(it.hasNext()) {
			RexInputRef inputRef = (RexInputRef) it.next();
			fields[i++] = StratosphereRelUtils.getTypeClass(inputRef.getType());
		}
		return fields;
	}

	
}
