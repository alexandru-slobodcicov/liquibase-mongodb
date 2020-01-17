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
import liquibase.exception.DatabaseException;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class FindAllStatementIT extends AbstractMongoIntegrationTest {

    @ParameterizedTest
    @ValueSource(classes = {Document.class, Map.class})
    <T> void queryForList(final Class<T> clazz) throws DatabaseException {
        final MongoDatabase database = mongoConnection.getDb();
        final List<Document> testObjects = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Collections.singletonMap("id", (Object) id))
            .map(Document::new)
            .collect(Collectors.toList());
        database.createCollection(COLLECTION_NAME_1);
        database.getCollection(COLLECTION_NAME_1).insertMany(testObjects);

        final FindAllStatement statement = new FindAllStatement(COLLECTION_NAME_1);
        assertThat(statement.queryForList(database, clazz))
            .hasSize(5);
    }

    @Test
    void toStringJs() {
        final FindAllStatement statement = new FindAllStatement(COLLECTION_NAME_1);
        assertThat(statement.toJs())
            .isEqualTo(statement.toString())
            .isEqualTo("db.collectionName.find({});");
    }
}
