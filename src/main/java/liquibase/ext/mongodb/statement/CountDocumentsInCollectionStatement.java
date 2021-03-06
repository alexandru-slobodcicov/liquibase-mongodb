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

import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.nosql.statement.NoSqlQueryForLongStatement;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.conversions.Bson;

import java.util.Objects;

import static java.util.Optional.ofNullable;
import static liquibase.ext.mongodb.statement.AbstractRunCommandStatement.SHELL_DB_PREFIX;

@Getter
@EqualsAndHashCode(callSuper = true)
public class CountDocumentsInCollectionStatement extends AbstractCollectionStatement
        implements NoSqlQueryForLongStatement<MongoLiquibaseDatabase> {

    public static final String COMMAND_NAME = "countDocuments";

    private final Bson filter;

    public CountDocumentsInCollectionStatement(final String collectionName) {
        this(collectionName, null);
    }

    public CountDocumentsInCollectionStatement(final String collectionName, final Bson filter) {
        super(collectionName);
        this.filter = filter;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public String toJs() {
        return
                SHELL_DB_PREFIX +
                        getCollectionName() +
                        "." +
                        getCommandName() +
                        "(" +
                        ofNullable(filter).map(Objects::toString).orElse(null) +
                        ");";
    }

    @Override
    public long queryForLong(final MongoLiquibaseDatabase database) {
        return database.getMongoDatabase().getCollection(getCollectionName()).countDocuments(filter);
    }

}
