package liquibase.ext.mongodb.statement;

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

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import liquibase.exception.DatabaseException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;

@Getter
@EqualsAndHashCode(callSuper = true)
public class InsertOneStatement extends AbstractMongoStatement {

    public static final String COMMAND = "insertOne";

    private final String collectionName;
    private final Document document;
    private final Document options;

    public InsertOneStatement(final String collectionName, final String document, final String options) {
        this(collectionName, orEmptyDocument(document), orEmptyDocument(options));
    }

    public InsertOneStatement(final String collectionName, final Document document, final Document options) {
        this.collectionName = collectionName;
        this.document = document;
        this.options = options;
    }

    @Override
    public String toJs() {
        return
                "db." +
                        collectionName +
                        "." +
                        COMMAND +
                        "(" +
                        document.toJson() +
                        ", " +
                        options.toJson() +
                        ");";
    }

    @Override
    public void execute(MongoDatabase db) throws DatabaseException {
        try {
            final MongoCollection<Document> collection = db.getCollection(collectionName);
            collection.insertOne(document);
            //TODO: Parse options into POJO InsertOneOptions.class
        } catch (MongoException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}
