package liquibase.ext.mongodb.executor;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.AbstractExecutor;
import liquibase.ext.mongodb.database.MongoConnection;
import liquibase.ext.mongodb.statement.AbstractMongoDocumentStatement;
import liquibase.ext.mongodb.statement.AbstractMongoStatement;
import liquibase.logging.LogService;
import liquibase.logging.LogType;
import liquibase.servicelocator.LiquibaseService;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogLockStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static liquibase.ext.mongodb.database.MongoLiquibaseDatabase.DATABASE_CHANGE_LOG_LOCK_TABLE_NAME;

@NoArgsConstructor
@LiquibaseService
public class MongoExecutor extends AbstractExecutor {

    @Getter
    @Setter
    public MongoDatabase db;

    public void setDatabase(Database database) {
        super.setDatabase(database);
        db = ((MongoConnection) this.database.getConnection()).getDb();
    }

    @Override
    public <T> T queryForObject(final SqlStatement sql, final Class<T> requiredType) throws DatabaseException {
        if (sql instanceof SelectFromDatabaseChangeLogLockStatement) {
            final SelectFromDatabaseChangeLogLockStatement statement = (SelectFromDatabaseChangeLogLockStatement) sql;
            final List<String> fieldsToSelect =
                Stream.of(statement.getColumnsToSelect()).map(ColumnConfig::getName)
                    .collect(Collectors.toList());

            return (T) db.getCollection(DATABASE_CHANGE_LOG_LOCK_TABLE_NAME)
                .find()
                .projection(Projections.include(fieldsToSelect)).first();
        }

        return null;
    }

    @Override
    public <T> T queryForObject(final SqlStatement sql, final Class<T> requiredType, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof AbstractMongoStatement) {
            return ((AbstractMongoStatement) sql).queryForObject(db, requiredType);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public long queryForLong(final SqlStatement sql) throws DatabaseException {
        if (sql instanceof AbstractMongoStatement) {
            return ((AbstractMongoStatement) sql).queryForLong(db);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public long queryForLong(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        return 0;
    }

    @Override
    public int queryForInt(final SqlStatement sql) throws DatabaseException {
        return 0;
    }

    @Override
    public int queryForInt(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        return 0;
    }

    @Override
    public List queryForList(final SqlStatement sql, final Class elementType) throws DatabaseException {
        if (sql instanceof AbstractMongoStatement) {
            return ((AbstractMongoStatement) sql).queryForList(db, elementType);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public List queryForList(final SqlStatement sql, final Class elementType, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        return null;
    }

    @Override
    public List<Map<String, ?>> queryForList(final SqlStatement sql) throws DatabaseException {
        return null;
    }

    @Override
    public List<Map<String, ?>> queryForList(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        return null;
    }

    @Override
    @SneakyThrows
    public void execute(final SqlStatement sql) throws DatabaseException {
        this.execute(sql, emptyList());
    }

    @Override
    public void execute(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof AbstractMongoStatement) {
            ((AbstractMongoStatement) sql).execute(db);
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public int update(final SqlStatement sql) throws DatabaseException {
        if (sql instanceof AbstractMongoStatement) {
            return ((AbstractMongoStatement) sql).update(db);
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public int update(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        return 0;
    }

    @Override
    public void comment(final String message) throws DatabaseException {
        LogService.getLog(getClass()).debug(LogType.LOG, message);
    }

    @Override
    public boolean updatesDatabase() {
        return true;
    }

    public <T> T run(final SqlStatement sql) throws DatabaseException {
        if (sql instanceof AbstractMongoDocumentStatement) {
            return (T) ((AbstractMongoDocumentStatement) sql).run(db);
        } else {
            throw new IllegalArgumentException();
        }
    }

}
