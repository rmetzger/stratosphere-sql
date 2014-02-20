package eu.stratosphere.sql;

import java.util.ArrayList;
import java.util.List;

import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeFieldImpl;
import org.eigenbase.reltype.RelDataTypeImpl;
import org.eigenbase.sql.type.SqlTypeName;

public class StratosphereRelType extends RelDataTypeImpl {

	
	private RelDataTypeFactory typeFactory;
	List<RelDataTypeField> fields;
	List<String> fieldNames = new ArrayList<String>();
	
	public StratosphereRelType() {
		this.digest = "StratosphereSQLRow";
		typeFactory = new JavaTypeFactoryImpl();
		fields = getFieldList();
	}
	@Override
	public List<RelDataTypeField> getFieldList() {
		if(fields == null) {
			List<RelDataTypeField> ret = new ArrayList<RelDataTypeField>();
			RelDataTypeFieldImpl defaultField = new RelDataTypeFieldImpl("*", 0, typeFactory.createSqlType(SqlTypeName.ANY));
			ret.add(defaultField);
			fieldNames.add("*");
			fields = ret;
		} 
        return fields;
	}
	@Override
	protected void generateTypeString(StringBuilder sb, boolean withDetail) {
		throw new RuntimeException("Here we go agaiN");
	}

	@Override
	public RelDataTypeField getField(String fieldName, boolean caseSensitive) {
		for(int i = 0; i < fieldNames.size(); i++) {
			if(fieldNames.get(i).equals(fieldName)) {
				return fields.get(i);
			}
		}
		throw new RuntimeException("unknow field");
	}
	
	@Override
	public int getFieldCount() {
		return fields.size();
	}
}
