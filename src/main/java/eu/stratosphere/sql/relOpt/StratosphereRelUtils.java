package eu.stratosphere.sql.relOpt;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeName;

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
}
