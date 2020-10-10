package liquibase.ext.mongodb.statement;

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

import liquibase.ext.mongodb.database.MongoConnection;
import liquibase.nosql.statement.AbstractNoSqlStatement;
import liquibase.nosql.statement.NoSqlExecuteStatement;
import org.bson.Document;

public abstract class AbstractMongoDocumentStatement<T extends Document> extends AbstractNoSqlStatement
        implements NoSqlExecuteStatement<MongoConnection> {

    public abstract T run(MongoConnection connection);

    @Override
    public void execute(final MongoConnection connection) {
        run(connection);
    }

}
