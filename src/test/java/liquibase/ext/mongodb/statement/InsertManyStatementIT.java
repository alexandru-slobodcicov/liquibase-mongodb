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

import com.mongodb.client.MongoDatabase;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class InsertManyStatementIT extends AbstractMongoIntegrationTest {

    @Test
    void toStringTest() {
        final InsertManyStatement statement = new InsertManyStatement(COLLECTION_NAME_1, Collections.emptyList(), new Document());
        assertThat(statement.toJs())
            .isEqualTo(statement.toString())
            .isEqualTo("db.collectionName.insertMany([], {});");
    }

    @Test
    void executeForList() {
        final MongoDatabase database = connection.getDatabase();
        final List<Document> testObjects = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Collections.singletonMap("id", (Object) id))
            .map(Document::new)
            .collect(Collectors.toList());
        new InsertManyStatement(COLLECTION_NAME_1, testObjects, new Document()).execute(connection);

        assertThat(database.getCollection(COLLECTION_NAME_1).find())
            .hasSize(5);
    }

    @Test
    void executeForString() {
        final MongoDatabase database = connection.getDatabase();
        final String testObjects = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Collections.singletonMap("id", (Object) id))
            .map(Document::new)
            .map(Document::toJson)
            .collect(Collectors.joining(",", "[", "]"));
        new InsertManyStatement(COLLECTION_NAME_1, testObjects, "").execute(connection);

        assertThat(database.getCollection(COLLECTION_NAME_1).find())
            .hasSize(5);
    }
}
