/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 * Copyright 2011-2012 CELI srl
 * Copyright 2011-2012 TXT e-solutions SpA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.jdbc.common.OrientJdbcConstants;
import com.orientechnologies.orient.jdbc.connection.OrientNativeGraphConnection;
import com.orientechnologies.orient.jdbc.connection.OrientTinkerpopGraphConnection;

/**
 * @author Roberto Franchini (CELI srl - franchini--at--celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvatore.piccione AT network.txtgroup.com)
 */
public class OrientJdbcDatabaseMetaData implements DatabaseMetaData {
    private final OrientJdbcConnection connection;
    private final ODatabaseRecord database;
    private final OMetadata metadata;

    public OrientJdbcDatabaseMetaData(OrientJdbcConnection iConnection) throws SQLException {
        connection = iConnection;
        if (iConnection.isWrapperFor(ODatabaseRecord.class)) {
            database = iConnection.unwrap(ODatabaseRecord.class);
            metadata = database.getMetadata();
        }
        else {
            //FIXME add support for ObjectDatabase
            database = null;//iConnection.unwrap(ODatabaseObject.class);
            metadata = null;
        }
    }

    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        //TODO Check with Luca if this is true
        return true;
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        //TODO TO BE IMPLEMENTED
        return null;
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        //TODO TO BE IMPLEMENTED
        return null;
    }

    public String getCatalogSeparator() throws SQLException {
        //the database does not use catalogs
        return null;
    }

    public String getCatalogTerm() throws SQLException {
        //the database does not use catalogs
        return null;
    }

    public ResultSet getCatalogs() throws SQLException {
        //the database does not use catalogs
        return null;
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        //TODO TO BE IMPLEMENTED
        return null;
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        //TODO TO BE IMPLEMENTED
        return null;
    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        //TODO TO BE IMPLEMENTED
        return null;
    }

    public Connection getConnection() throws SQLException {
        return connection;
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        //OrientDB misses the concept of foreign key
        return null;
    }

    public int getDatabaseMajorVersion() throws SQLException {
        return Integer.valueOf(OConstants.ORIENT_VERSION.split("\\.")[0]);
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return Integer.valueOf(OConstants.ORIENT_VERSION.split("\\.")[1].substring(0, 1));
    }

    public String getDatabaseProductName() throws SQLException {
        return OrientJdbcConstants.DATABASE_PRODUCT_NAME;
    }

    public String getDatabaseProductVersion() throws SQLException {
        return OConstants.getVersion();
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        //TODO check with Luca
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    public int getDriverMajorVersion() {
        return OrientJdbcDriver.MAJOR_VERSION;
    }

    public int getDriverMinorVersion() {
        return OrientJdbcDriver.MINOR_VERSION;
    }

    public String getDriverName() throws SQLException {
        return OrientJdbcConstants.DRIVER_NAME;
    }

    public String getDriverVersion() throws SQLException {
        return OrientJdbcDriver.getVersion();
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        //OrientDB misses the concept of foreign key
        return null;
    }

    public String getExtraNameCharacters() throws SQLException {
        //TODO check with Luca
        return "";
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        //maybe ORecordHook??
        return null;
    }

    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        //maybe ORecordHook??
        return null;
    }

    public String getIdentifierQuoteString() throws SQLException {
        //TODO check with Luca
        return " ";
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        //OrientDB misses the concept of foreign key
        return null;
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        //TODO TO BE IMPLEMENTED
        return null;
    }

    public int getJDBCMajorVersion() throws SQLException {
        return OrientJdbcConstants.MAJOR_JDBC_VERSION;
    }

    public int getJDBCMinorVersion() throws SQLException {
        return OrientJdbcConstants.MINOR_JDBC_VERSION;
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    public int getMaxConnections() throws SQLException {
        return 0;
    }

    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    public int getMaxStatements() throws SQLException {
        return 0;
    }

    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    public String getNumericFunctions() throws SQLException {
        return null;
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        Set<OIndex<?>> classIndexes = metadata.getIndexManager().getClassIndexes(table);

        Set<OIndex<?>> uniqueIndexes = new HashSet<OIndex<?>>();

        for (OIndex<?> oIndex : classIndexes) {
            if (oIndex.getType().equals(INDEX_TYPE.UNIQUE.name())) uniqueIndexes.add(oIndex);
        }

        List<ODocument> iRecords = new ArrayList<ODocument>();

        for (OIndex<?> unique : uniqueIndexes) {
            int keyFiledSeq = 1;
            for (String keyFieldName : unique.getDefinition().getFields()) {
                ODocument doc = new ODocument();
                doc.field("TABLE_CAT", catalog);
                doc.field("TABLE_SCHEMA", schema);
                doc.field("TABLE_NAME", table);
                doc.field("COLUMN_NAME", keyFieldName);
                doc.field("KEY_SEQ", Integer.valueOf(keyFiledSeq), OType.INTEGER);
                doc.field("PK_NAME", unique.getName());
                keyFiledSeq++;

                iRecords.add(doc);
            }

        }
        
        ResultSet result = new OrientJdbcResultSet((OrientJdbcStatement) connection.createStatement(), iRecords, 
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT, OrientJdbcResultSet.DEFAULT_FETCH_DIRECTION, false);
        return result;
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        //waiting for implementation. See http://code.google.com/p/orient/issues/detail?id=4
        return null;
    }

    public String getProcedureTerm() throws SQLException {
        //waiting for implementation. See http://code.google.com/p/orient/issues/detail?id=4
        return null;
    }

    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        //waiting for implementation. See http://code.google.com/p/orient/issues/detail?id=4
        return null;
    }

    public int getResultSetHoldability() throws SQLException {
        return OrientJdbcResultSet.DEFAULT_RESULT_SET_HOLDABILITY;
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        //TODO to be further investigated
        return RowIdLifetime.ROWID_VALID_OTHER;
    }

    public String getSQLKeywords() throws SQLException {
        //TODO to be further investigated
        return "";
    }

    public int getSQLStateType() throws SQLException {
	    return DatabaseMetaData.sqlStateXOpen;
    }

    public String getSchemaTerm() throws SQLException {
        if (connection instanceof OrientNativeGraphConnection || 
            connection instanceof OrientTinkerpopGraphConnection)
            return "graph";
        else
            return "database";
    }

    public ResultSet getSchemas() throws SQLException {
        //TODO to be further investigated
        return null;
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        //TODO to be further investigated
        return null;
    }

    public String getSearchStringEscape() throws SQLException {
        //TODO to be further investigated
        return null;
    }

    public String getStringFunctions() throws SQLException {
        //TODO to be further investigated
        return null;
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        //TODO to be further investigated
        return null;
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        //TODO to be further investigated
        return null;
    }

    public String getSystemFunctions() throws SQLException {
        //TODO to be further investigated
        return null;
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        //TODO to be implemented
        return null;
    }

    public ResultSet getTableTypes() throws SQLException {
        List<ODocument> records = new ArrayList<ODocument>();
        records.add(new ODocument().field("TABLE_TYPE", "TABLE"));

        ResultSet result = new OrientJdbcResultSet((OrientJdbcStatement) connection.createStatement(), records, 
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT, OrientJdbcResultSet.DEFAULT_FETCH_DIRECTION, false);

        return result;
    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        //TODO to be implemented
        return null;
    }

    public String getTimeDateFunctions() throws SQLException {
        //TODO to be implemented
        return null;
    }

    public ResultSet getTypeInfo() throws SQLException {
        //TODO to be implemented
        return null;
    }

    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        //TODO to be implemented
        return null;
    }

    public String getURL() throws SQLException {
        return database.getURL();
    }

    public String getUserName() throws SQLException {
        return database.getUser().getName();
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        //TODO to be implemented
        return null;
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        return true;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean nullsAreSortedLow() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    public boolean supportsConvert() throws SQLException {
        return false;
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return true;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        //TODO to be further elaborated
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        //TODO to be checked
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        //TODO to be checked
        return false;
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsPositionedDelete() throws SQLException {
        //TODO to be further investigated
        return false;
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        //TODO to be futher investigated
        return false;
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        if (supportsResultSetType(type)) {
            switch (concurrency) {
                case ResultSet.CONCUR_READ_ONLY:
                    return true;
                case ResultSet.CONCUR_UPDATABLE:
                    return false; //TODO to be implemented!
                default:
                    throw new SQLException(ErrorMessages.get(
                            "ResultSet.badConcurrency", ResultSet.CONCUR_READ_ONLY + ", " + ResultSet.CONCUR_UPDATABLE, concurrency));
            }
        } else
            return false;
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        //TODO Check with Luca if this is right
        switch (holdability) {
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                return true; //this is the default behavior
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
                return false; //TODO to be implemented!
            default:
                throw new SQLException(ErrorMessages.get(
                        "ResultSet.badHoldability", ResultSet.CLOSE_CURSORS_AT_COMMIT + ", " + ResultSet.HOLD_CURSORS_OVER_COMMIT, holdability));
        }
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        //TODO Check with Luca if this is right
        switch (type) {
            case ResultSet.TYPE_FORWARD_ONLY:
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
                return true;
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                return false;
            default:
                throw new SQLException(ErrorMessages.get("ResultSet.badType",ResultSet.TYPE_FORWARD_ONLY + ", " + 
                        ResultSet.TYPE_SCROLL_INSENSITIVE + ", " + ResultSet.TYPE_SCROLL_SENSITIVE, type));
        }
    }

    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    public boolean supportsStatementPooling() throws SQLException {
        //TODO to be implemented
        return true;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    public boolean supportsStoredProcedures() throws SQLException {
        //waiting for implementation. See http://code.google.com/p/orient/issues/detail?id=4
        return false;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        //TODO to be checked with Luca
        return false;
    }

    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    public boolean supportsUnion() throws SQLException {
        return false;
    }

    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        //TODO to be implemented
        return false;
    }

    public boolean usesLocalFiles() throws SQLException {
        //TODO to be implemented
        return false;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
    	if (iface == null)
            throw new SQLException(ErrorMessages.get("Wrapper.wrappedClassIsNull"));
        return iface.isInstance(metadata);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
    	if (iface == null)
            throw new SQLException(ErrorMessages.get("Wrapper.wrappedClassIsNull"));
    	try {
            return iface.cast(database);
        } catch (ClassCastException e) {
            throw new SQLException(e);
        }
    }
}
