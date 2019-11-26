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

import java.util.ArrayList;
import java.util.List;

import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;
import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyList;

@Getter
@EqualsAndHashCode(callSuper = true)
public class InsertManyStatement extends AbstractMongoStatement {

    public static final String COMMAND = "insertMany";

    private final String collectionName;
    private final List<Document> documents;
    private Document options;

    public InsertManyStatement(final String collectionName, final String documents, final String options) {
        this(collectionName, new ArrayList<>(orEmptyList(documents)), orEmptyDocument(options));
    }

    public InsertManyStatement(final String collectionName, final List<Document> documents, final Document options) {
        this.collectionName = collectionName;
        this.documents = documents;
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
                        documents.toString() +
                        ", " +
                        options.toJson() +
                        ");";
    }

    @Override
    public void execute(MongoDatabase db) throws DatabaseException {
        try {
            final MongoCollection<Document> collection = db.getCollection(collectionName);
            collection.insertMany(documents);
            //TODO: Parse options into POJO InsertManyOptions.class
        } catch (MongoException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}
