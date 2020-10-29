package liquibase.ext.mongodb.lockservice;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2019 Mastercard
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
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

import com.mongodb.client.model.Filters;
import liquibase.ext.mongodb.database.MongoConnection;
import liquibase.ext.mongodb.statement.AbstractCollectionStatement;
import liquibase.nosql.statement.NoSqlQueryForObjectStatement;

public class SelectChangeLogLockStatement extends AbstractCollectionStatement
implements NoSqlQueryForObjectStatement<MongoConnection> {

    public static final String COMMAND_NAME = "findLock";

    public SelectChangeLogLockStatement(final String collectionName) {
        super(collectionName);
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public String toJs() {
        return
                "db." +
                        getCollectionName() +
                        "." +
                        getCommandName() +
                        "(" +
                        ");";
    }

    @Override
    public <T> T queryForObject(final MongoConnection connection, final Class<T> requiredType) {

        return connection.getDatabase().getCollection(getCollectionName(), requiredType)
                .find(Filters.eq(MongoChangeLogLock.Fields.id, 1)).first();
    }
}
