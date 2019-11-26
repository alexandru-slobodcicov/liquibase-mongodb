package liquibase.ext.mongodb.executor;

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

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import liquibase.change.ColumnConfig;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
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
import liquibase.statement.core.UpdateStatement;
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
    public <T> T queryForObject(final SqlStatement sql, final Class<T> requiredType) {
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
    public <T> T queryForObject(final SqlStatement sql, final Class<T> requiredType, final List<SqlVisitor> sqlVisitors) {
        if (sql instanceof AbstractMongoStatement) {
            return ((AbstractMongoStatement) sql).queryForObject(db, requiredType);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public long queryForLong(final SqlStatement sql) {
        if (sql instanceof AbstractMongoStatement) {
            return ((AbstractMongoStatement) sql).queryForLong(db);
        }
        throw new IllegalArgumentException();
    }

    @Override
    public long queryForLong(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) {
        return 0;
    }

    @Override
    public int queryForInt(final SqlStatement sql) {
        return 0;
    }

    @Override
    public int queryForInt(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) {
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
    public List queryForList(final SqlStatement sql, final Class elementType, final List<SqlVisitor> sqlVisitors) {
        return null;
    }

    @Override
    public List<Map<String, ?>> queryForList(final SqlStatement sql) {
        return null;
    }

    @Override
    public List<Map<String, ?>> queryForList(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) {
        return null;
    }

    @Override
    @SneakyThrows
    public void execute(final SqlStatement sql) {
        this.execute(sql, emptyList());
    }

    @Override
    public void execute(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof AbstractMongoStatement) {
            ((AbstractMongoStatement) sql).execute(db);
        } else if (isClearChecksumStatement(sql)) {
            try {
                ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(this.database).clearAllCheckSums();
            } catch (LiquibaseException e) {
                e.printStackTrace();
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public int update(final SqlStatement sql) {
        if (sql instanceof AbstractMongoStatement) {
            return ((AbstractMongoStatement) sql).update(db);
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public int update(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) {
        return 0;
    }

    @Override
    public void comment(final String message) {
        LogService.getLog(getClass()).debug(LogType.LOG, message);
    }

    @Override
    public boolean updatesDatabase() {
        return true;
    }

    public <T> T run(final SqlStatement sql) {
        if (sql instanceof AbstractMongoDocumentStatement) {
            return (T) ((AbstractMongoDocumentStatement) sql).run(db);
        } else {
            throw new IllegalArgumentException();
        }
    }

    private static boolean isClearChecksumStatement(final SqlStatement sqlStatement) {
        return sqlStatement instanceof UpdateStatement
            && ((UpdateStatement) sqlStatement).getNewColumnValues().containsKey("MD5SUM");
    }
}
