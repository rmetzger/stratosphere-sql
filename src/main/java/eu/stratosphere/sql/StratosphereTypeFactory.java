package eu.stratosphere.sql;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map.Entry;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.sql.type.SqlTypeName;

public class StratosphereTypeFactory implements RelDataTypeFactory {

	@Override
	public RelDataType createJavaType(Class clazz) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createJoinType(RelDataType... types) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createStructType(RelDataType[] types, String[] fieldNames) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createStructType(List<RelDataType> typeList,
			List<String> fieldNameList) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createStructType(FieldInfo fieldInfo) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createStructType(
			List<? extends Entry<String, RelDataType>> fieldList) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createArrayType(RelDataType elementType,
			long maxCardinality) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createMapType(RelDataType keyType, RelDataType valueType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createMultisetType(RelDataType elementType,
			long maxCardinality) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType copyType(RelDataType type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createTypeWithNullability(RelDataType type,
			boolean nullable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createTypeWithCharsetAndCollation(RelDataType type,
			Charset charset, SqlCollation collation) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Charset getDefaultCharset() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType leastRestrictive(List<RelDataType> types) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createSqlType(SqlTypeName typeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createSqlType(SqlTypeName typeName, int precision) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createSqlType(SqlTypeName typeName, int precision,
			int scale) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createSqlIntervalType(
			SqlIntervalQualifier intervalQualifier) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RelDataType createDecimalProduct(RelDataType type1, RelDataType type2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean useDoubleMultiplication(RelDataType type1, RelDataType type2) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public RelDataType createDecimalQuotient(RelDataType type1,
			RelDataType type2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FieldInfoBuilder builder() {
		// TODO Auto-generated method stub
		return null;
	}

}
