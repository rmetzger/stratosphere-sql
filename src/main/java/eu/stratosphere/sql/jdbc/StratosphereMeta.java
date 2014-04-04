package eu.stratosphere.sql.jdbc;

import java.sql.ResultSet;
import java.util.List;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.Cursor;
import net.hydromatic.avatica.Meta;
import net.hydromatic.optiq.runtime.ObjectEnumeratorCursor;

public class StratosphereMeta implements Meta {

	@Override
	public String getSqlKeywords() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNumericFunctions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStringFunctions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSystemFunctions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTimeDateFunctions() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getTables(String catalog, Pat schemaPattern,
			Pat tableNamePattern, List<String> typeList) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getColumns(String catalog, Pat schemaPattern,
			Pat tableNamePattern, Pat columnNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getSchemas(String catalog, Pat schemaPattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getCatalogs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getTableTypes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getProcedures(String catalog, Pat schemaPattern,
			Pat procedureNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, Pat schemaPattern,
			Pat procedureNamePattern, Pat columnNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema,
			String table, Pat columnNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, Pat schemaPattern,
			Pat tableNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema,
			String table, int scope, boolean nullable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema,
			String table) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog,
			String parentSchema, String parentTable, String foreignCatalog,
			String foreignSchema, String foreignTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getTypeInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
			boolean unique, boolean approximate) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getUDTs(String catalog, Pat schemaPattern,
			Pat typeNamePattern, int[] types) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, Pat schemaPattern,
			Pat typeNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getSuperTables(String catalog, Pat schemaPattern,
			Pat tableNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getAttributes(String catalog, Pat schemaPattern,
			Pat typeNamePattern, Pat attributeNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getClientInfoProperties() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getFunctions(String catalog, Pat schemaPattern,
			Pat functionNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, Pat schemaPattern,
			Pat functionNamePattern, Pat columnNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, Pat schemaPattern,
			Pat tableNamePattern, Pat columnNamePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Cursor createCursor(AvaticaResultSet resultSet) {
		System.err.println("CreateCursor");
		return new StratosphereCursor( (StratosphereResultSet) resultSet);
	}

	@Override
	public AvaticaPrepareResult prepare(AvaticaStatement statement, String sql) {
		return new StratospherePrepareResult(statement, sql);
	}

}
