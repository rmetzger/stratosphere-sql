package eu.stratosphere.sql;

import java.util.Iterator;

import org.eigenbase.relopt.RelOptRule;

import com.google.common.collect.ImmutableSet;

import net.hydromatic.optiq.tools.RuleSet;

public class StratosphereRuleSet implements RuleSet {
	final ImmutableSet<RelOptRule> rules;
	
	public StratosphereRuleSet(ImmutableSet<RelOptRule> rules) {
		this.rules = rules;
	}

	@Override
	public Iterator<RelOptRule> iterator() {
		return rules.iterator();
	}

}
