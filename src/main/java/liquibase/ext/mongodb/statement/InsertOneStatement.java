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

import com.mongodb.client.MongoCollection;
import liquibase.ext.mongodb.database.MongoConnection;
import liquibase.nosql.statement.NoSqlExecuteStatement;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import static java.util.Optional.ofNullable;
import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;

@Getter
@EqualsAndHashCode(callSuper = true)
public class InsertOneStatement extends AbstractCollectionStatement
implements NoSqlExecuteStatement<MongoConnection> {

    public static final String COMMAND_NAME = "insertOne";

    private final Document document;
    private final Document options;

    public InsertOneStatement(final String collectionName, final String document, final String options) {
        this(collectionName, orEmptyDocument(document), orEmptyDocument(options));
    }

    public InsertOneStatement(final String collectionName, final Document document, final Document options) {
        super(collectionName);
        this.document = document;
        this.options = options;
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
                        ofNullable(document).map(Document::toJson).orElse(null) +
                        ", " +
                        ofNullable(options).map(Document::toJson).orElse(null) +
                        ");";
    }

    @Override
    public void execute(final MongoConnection connection) {
            final MongoCollection<Document> collection = connection.getDatabase().getCollection(getCollectionName());
            collection.insertOne(document);
    }

}
