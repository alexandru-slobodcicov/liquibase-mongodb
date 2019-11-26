package liquibase.ext.mongodb.database;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2019 Mastercard
 * %%
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
 * #L%
 */

import liquibase.CatalogAndSchema;
import liquibase.change.Change;
import liquibase.configuration.ConfigurationProperty;
import liquibase.configuration.GlobalConfiguration;
import liquibase.configuration.LiquibaseConfiguration;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.structure.DatabaseObject;
import lombok.Setter;

import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.util.List;

public class MongoLiquibaseDatabase extends AbstractJdbcDatabase {

    public static final String MONGODB_PRODUCT_NAME = "MongoDB";
    public static final String MONGODB_PRODUCT_SHORT_NAME = "mongodb";
    public static final String DATABASE_CHANGE_LOG_TABLE_NAME = "databaseChangeLog";
    public static final String DATABASE_CHANGE_LOG_LOCK_TABLE_NAME = "databaseChangeLogLock";

    @Setter
    private String databaseChangeLogTableName;

    @Setter
    private String databaseChangeLogLockTableName;

    @Setter
    private String liquibaseCatalogName;
    private MongoConnection connection;

    public MongoLiquibaseDatabase() {
        databaseChangeLogTableName = DATABASE_CHANGE_LOG_TABLE_NAME;
        databaseChangeLogLockTableName = DATABASE_CHANGE_LOG_LOCK_TABLE_NAME;
    }

    @Override
    public MongoConnection getConnection() {
        return connection;
    }

    public void setConnection(final DatabaseConnection connection) {
        this.connection = (MongoConnection) connection;
    }

    @Override
    public String getDatabaseChangeLogTableName() {
        if (databaseChangeLogTableName != null) {
            return databaseChangeLogTableName;
        }

        return LiquibaseConfiguration.getInstance().getConfiguration(GlobalConfiguration.class).getDatabaseChangeLogTableName();
    }

    @Override
    public void setDatabaseChangeLogTableName(final String tableName) {
        this.databaseChangeLogTableName = tableName;
    }

    @Override
    public String getDatabaseChangeLogLockTableName() {
        if (databaseChangeLogLockTableName != null) {
            return databaseChangeLogLockTableName;
        }

        return LiquibaseConfiguration.getInstance().getConfiguration(GlobalConfiguration.class).getDatabaseChangeLogLockTableName();
    }

    public String getShortName() {
        return MONGODB_PRODUCT_SHORT_NAME;
    }

    public String getDefaultCatalogName() {
        return null;
    }

    public void setDefaultCatalogName(String catalogName) {

    }

    public String getDefaultSchemaName() {
        return null;
    }

    public void setDefaultSchemaName(String schemaName) {
    }

    public Integer getDefaultScaleForNativeDataType(String nativeDataType) {
        return null;
    }

    public Integer getDefaultPort() {
        return 27017;
    }

    public Integer getFetchSize() {
        return null;
    }

    public String getLiquibaseCatalogName() {
        if (liquibaseCatalogName != null) {
            return liquibaseCatalogName;
        }

        ConfigurationProperty configuration = LiquibaseConfiguration.getInstance().getProperty(GlobalConfiguration.class, GlobalConfiguration.LIQUIBASE_CATALOG_NAME);
        if (configuration.getWasOverridden()) {
            return configuration.getValue(String.class);
        }

        return getDefaultCatalogName();
    }

    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    @Override
    public boolean supportsDropTableCascadeConstraints() {
        return false;
    }

    @Override
    public boolean supportsAutoIncrement() {
        return false;
    }

    @Override
    public String getLineComment() {
        return "//";
    }

    @Override
    public String getAutoIncrementClause(BigInteger startWith, BigInteger incrementBy) {
        return null;
    }

    @Override
    public boolean isSystemObject(DatabaseObject example) {
        return false;
    }

    @Override
    public boolean isLiquibaseObject(DatabaseObject object) {
        return false;
    }

    @Override
    public String getViewDefinition(CatalogAndSchema schema, String name) throws DatabaseException {
        return null;
    }

    public String escapeObjectName(String catalogName, String schemaName, String objectName, Class<? extends DatabaseObject> objectType) {
        return null;
    }

    public String escapeTableName(String catalogName, String schemaName, String tableName) {
        return null;
    }

    public String escapeIndexName(String catalogName, String schemaName, String indexName) {
        return null;
    }

    public String escapeObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
        return null;
    }

    public String escapeColumnName(String catalogName, String schemaName, String tableName, String columnName) {
        return null;
    }

    public String escapeColumnName(String catalogName, String schemaName, String tableName, String columnName, boolean quoteNamesThatMayBeFunctions) {
        return null;
    }

    public String escapeColumnNameList(String columnNames) {
        return null;
    }

    public boolean supportsTablespaces() {
        return false;
    }

    public boolean supportsCatalogs() {
        return false;
    }

    public CatalogAndSchema.CatalogAndSchemaCase getSchemaAndCatalogCase() {
        return CatalogAndSchema.CatalogAndSchemaCase.ORIGINAL_CASE;
    }

    public boolean supportsSchemas() {
        return false;
    }

    public boolean supportsCatalogInObjectName(Class<? extends DatabaseObject> type) {
        return false;
    }

    public String generatePrimaryKeyName(String tableName) {
        return null;
    }

    public String escapeSequenceName(String catalogName, String schemaName, String sequenceName) {
        return null;
    }

    public String escapeViewName(String catalogName, String schemaName, String viewName) {
        return null;
    }

    @Override
    public void commit() throws DatabaseException {

    }

    @Override
    public void rollback() throws DatabaseException {

    }

    public String escapeStringForDatabase(String string) {
        return null;
    }

    @Override
    public void close() throws DatabaseException {

    }

    @Override
    public boolean supportsRestrictForeignKeys() {
        return false;
    }

    public String escapeConstraintName(String constraintName) {
        return null;
    }

    @Override
    public boolean isSafeToRunUpdate() throws DatabaseException {
        //TODO: Add the check to not be admin, etc.
        return false;
    }

    public void saveStatements(Change change, List<SqlVisitor> sqlVisitors, Writer writer) throws IOException {

    }

    public void executeRollbackStatements(Change change, List<SqlVisitor> sqlVisitors) throws LiquibaseException {

    }

    public void executeRollbackStatements(SqlStatement[] statements, List<SqlVisitor> sqlVisitors) throws LiquibaseException {

    }

    public void saveRollbackStatement(Change change, List<SqlVisitor> sqlVisitors, Writer writer) throws IOException, LiquibaseException {

    }


    public List<DatabaseFunction> getDateFunctions() {
        return null;
    }


    public boolean supportsForeignKeyDisable() {
        return false;
    }

    public boolean disableForeignKeyChecks() throws DatabaseException {
        return false;
    }

    public void enableForeignKeyChecks() throws DatabaseException {

    }

    @Override
    public boolean isCaseSensitive() {
        return true;
    }

    @Override
    public boolean isReservedWord(String string) {
        return false;
    }

    public String correctObjectName(String name, Class<? extends DatabaseObject> objectType) {
        return null;
    }

    public boolean isFunction(String string) {
        return false;
    }

    public int getDataTypeMaxParameters(String dataTypeName) {
        return 0;
    }

    public CatalogAndSchema getDefaultSchema() {
        return null;
    }

    public boolean dataTypeIsNotModifiable(String typeName) {
        return false;
    }

    public String generateDatabaseFunctionValue(DatabaseFunction databaseFunction) {
        return null;
    }

    public ObjectQuotingStrategy getObjectQuotingStrategy() {
        return null;
    }

    public void setObjectQuotingStrategy(ObjectQuotingStrategy quotingStrategy) {

    }

    public boolean createsIndexesForForeignKeys() {
        return false;
    }

    public boolean getOutputDefaultSchema() {
        return false;
    }

    public void setOutputDefaultSchema(boolean outputDefaultSchema) {

    }

    public boolean isDefaultSchema(String catalog, String schema) {
        return false;
    }

    public boolean isDefaultCatalog(String catalog) {
        return false;
    }

    public boolean getOutputDefaultCatalog() {
        return false;
    }

    public void setOutputDefaultCatalog(boolean outputDefaultCatalog) {

    }

    public boolean supportsPrimaryKeyNames() {
        return false;
    }

    public boolean supportsNotNullConstraintNames() {
        return false;
    }

    public boolean supportsBatchUpdates() throws DatabaseException {
        return false;
    }

    public boolean requiresExplicitNullForColumns() {
        return false;
    }

    public String getSystemSchema() {
        return null;
    }

    public String escapeDataTypeName(String dataTypeName) {
        return null;
    }

    public String unescapeDataTypeName(String dataTypeName) {
        return null;
    }

    public String unescapeDataTypeString(String dataTypeString) {
        return null;
    }

    public ValidationErrors validate() {
        return null;
    }

    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    public String getDefaultDriver(String url) {
        if (url.startsWith("mongodb://")) {
            return "MongoClientDriver";
        }
        return null;
    }

    public boolean requiresUsername() {
        return false;
    }

    public boolean requiresPassword() {
        return false;
    }

    public boolean getAutoCommitMode() {
        return false;
    }

    public boolean supportsDDLInTransaction() {
        return false;
    }

    public String getDatabaseProductName() {
        return MONGODB_PRODUCT_NAME;
    }

    protected String getDefaultDatabaseProductName() {
        return "MongoDB";
    }

    public String getDatabaseProductVersion() throws DatabaseException {
        return null;
    }

    public int getDatabaseMajorVersion() throws DatabaseException {
        return 0;
    }

    public int getDatabaseMinorVersion() throws DatabaseException {
        return 0;
    }

    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        // If it looks like a MongoDB, swims like a MongoDB and quacks like a MongoDB,
        return getDatabaseProductName().equals(conn.getDatabaseProductName());
    }

    @Override
    public String toString() {
        return getShortName() + " Database";
    }

}
