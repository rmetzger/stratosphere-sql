package eu.stratosphere.sql.rules;

import java.util.Iterator;

import net.hydromatic.optiq.tools.RuleSet;

import org.eigenbase.relopt.RelOptRule;

import com.google.common.collect.ImmutableSet;

public class StratosphereRuleSet implements RuleSet {
	final private ImmutableSet<RelOptRule> rules;
	
	public StratosphereRuleSet(ImmutableSet<RelOptRule> rules) {
		this.rules = rules;
	}

	@Override
	public Iterator<RelOptRule> iterator() {
		return rules.iterator();
	}

}
