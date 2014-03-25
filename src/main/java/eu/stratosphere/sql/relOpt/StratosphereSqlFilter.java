package eu.stratosphere.sql.relOpt;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexExecutable;
import org.eigenbase.rex.RexExecutorImpl;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.sql.relOpt.filter.StratosphereSqlFilterMapOperator;

public class StratosphereSqlFilter	extends FilterRelBase implements StratosphereRel {
	public StratosphereSqlFilter(RelOptCluster cluster, RelTraitSet traits,
			RelNode child, RexNode condition) {
		super(cluster, traits, child, condition);
		Preconditions.checkArgument(getConvention() == CONVENTION);
	}

	@Override
	public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		System.err.println("StratosphereSqlFilter.copy()");
		return new StratosphereSqlFilter(getCluster(), traitSet, sole(inputs), getCondition());
	}
	
	@Override
	public Operator getStratosphereOperator() {
		Operator inputOp = StratosphereRelUtils.openSingleInputOperator(getInputs());
		RexNode cond = getCondition();
		
		final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final RexExecutorImpl executor = new RexExecutorImpl(null);
        StratosphereRexUtils.ReplaceInputRefVisitor replaceInputRefsByExternalInputRefsVisitor = new StratosphereRexUtils.ReplaceInputRefVisitor();
        cond.accept(replaceInputRefsByExternalInputRefsVisitor);
        
        final ImmutableList<RexNode> localExps = ImmutableList.of(cond);
		
		
        Set<StratosphereRexUtils.ProjectionFieldProperties> fields = new HashSet<StratosphereRexUtils.ProjectionFieldProperties>();
        int pos = 0;
    	for(Pair<Integer, RelDataType> rexInput : replaceInputRefsByExternalInputRefsVisitor.getInputPosAndType() ) {
        	StratosphereRexUtils.ProjectionFieldProperties field = new StratosphereRexUtils.ProjectionFieldProperties();
        	field.fieldIndex = pos++;
        	field.positionInInput = rexInput.getKey();
        	field.inFieldType = StratosphereRelUtils.getTypeClass(rexInput.getValue());
        	field.name = cond.toString();
        	fields.add(field);
    	}
        RexExecutable executable = executor.createExecutable(rexBuilder, localExps);
		System.err.println("Code: "+executable.getSource());
        
		Operator filter = MapOperator.builder(new StratosphereSqlFilterMapOperator(executable.getSource(), fields) )
									.input(inputOp)
									.name(condition.toString())
									.build();
		return filter;
	}

}
