package eu.stratosphere.sql;

import java.nio.charset.Charset;
import java.util.List;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeComparability;
import org.eigenbase.reltype.RelDataTypeFamily;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypePrecedenceList;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.sql.type.SqlTypeName;

public class FakeItType implements RelDataType {

	@Override
	public boolean isStruct() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public List<RelDataTypeField> getFieldList() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public List<String> getFieldNames() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public int getFieldCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public RelDataTypeField getField(String fieldName, boolean caseSensitive) {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public boolean isNullable() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public RelDataType getComponentType() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public RelDataType getKeyType() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public RelDataType getValueType() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public Charset getCharset() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public SqlCollation getCollation() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public SqlIntervalQualifier getIntervalQualifier() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public int getPrecision() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getScale() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public SqlTypeName getSqlTypeName() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public SqlIdentifier getSqlIdentifier() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public String getFullTypeString() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public RelDataTypeFamily getFamily() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public RelDataTypePrecedenceList getPrecedenceList() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

	@Override
	public RelDataTypeComparability getComparability() {
		// TODO Auto-generated method stub
		throw new RuntimeException("implMe");
	}

}
