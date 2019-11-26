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

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import liquibase.exception.DatabaseException;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class InsertOneStatementTest extends AbstractMongoIntegrationTest {

    @Test
    void executeForObject() throws DatabaseException {
        final MongoDatabase database = mongoConnection.getDb();
        final Document document = new Document("key1", "value1");
        new InsertOneStatement(COLLECTION_NAME_1, document, new Document()).execute(database);

        assertThat(database.getCollection(COLLECTION_NAME_1).find())
            .containsExactly(document);
    }

    @Test
    void executeForString() throws DatabaseException {
        final MongoDatabase database = mongoConnection.getDb();
        final Document document = new Document("key1", "value1");
        new InsertOneStatement(COLLECTION_NAME_1, document.toJson(), "").execute(database);

        final FindIterable<Document> docs = database.getCollection(COLLECTION_NAME_1).find();
        assertThat(docs).hasSize(1);
        assertThat(docs.iterator().next())
            .containsEntry("key1", "value1")
            .containsKey("_id");
    }

    @Test
    void toStringJs() {
        final InsertOneStatement statement = new InsertOneStatement(COLLECTION_NAME_1, new Document("key1", "value1"), new Document());
        assertThat(statement.toJs())
            .isEqualTo(statement.toString())
            .isEqualTo("db.collectionName.insertOne({\"key1\": \"value1\"}, {});");
    }
}
