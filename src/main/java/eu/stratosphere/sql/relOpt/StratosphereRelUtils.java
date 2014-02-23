package eu.stratosphere.sql.relOpt;

import java.util.List;

import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeName;

import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;

public class StratosphereRelUtils {


	public static Value newValue(RelDataType in) {
		if(in.getSqlTypeName() == SqlTypeName.INTEGER) {
			return new IntValue();
		}
		if(in.getSqlTypeName() == SqlTypeName.VARCHAR) {
			return new StringValue();
		}
		throw new RuntimeException("Unsupported type "+in);
	}

	public static Class<? extends Value> getTypeClass(RelDataType type) {
		if(type.getSqlTypeName() == SqlTypeName.INTEGER) {
			return IntValue.class;
		}
		if(type.getSqlTypeName() == SqlTypeName.VARCHAR) {
			return StringValue.class;
		}
		throw new RuntimeException("Unsupported type "+type);
	}
	
	public static Operator openSingleInputOperator(List<RelNode> optiqInput) {
		Operator inputOp = null;
		if(optiqInput.size() == 1) {
			RelNode optiqSingleInput = optiqInput.get(0);
			if(!(optiqSingleInput instanceof StratosphereRel)) {
				throw new RuntimeException("Input not properly converted to StratosphereRel");
			}
			inputOp = ( (StratosphereRel)optiqSingleInput).getStratosphereOperator();
		} else {
			throw new RuntimeException("Multiple inputs not supported at this time");
		}
		return inputOp;
	}
}
