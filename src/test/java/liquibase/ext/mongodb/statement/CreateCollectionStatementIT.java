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

import liquibase.ext.AbstractMongoIntegrationTest;
import org.junit.jupiter.api.Test;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static liquibase.ext.mongodb.TestUtils.EMPTY_OPTION;
import static liquibase.ext.mongodb.TestUtils.OPTION_1;
import static liquibase.ext.mongodb.TestUtils.getCollections;
import static org.assertj.core.api.Assertions.assertThat;

class CreateCollectionStatementIT extends AbstractMongoIntegrationTest {

    private static final String EXPECTED_COMMAND = "db.createCollection(collectionName, {\"opt1\": \"option 1\"});";

    @Test
    void execute() {
        final String collection = COLLECTION_NAME_1 + System.nanoTime();
        final CreateCollectionStatement statement = new CreateCollectionStatement(collection, EMPTY_OPTION);
        statement.execute(mongoConnection.getDb());
        assertThat(getCollections(mongoConnection))
            .contains(collection);
    }

    @Test
    void toStringJs() {
        final CreateCollectionStatement statement = new CreateCollectionStatement(COLLECTION_NAME_1, OPTION_1);
        assertThat(statement.toJs())
            .isEqualTo(EXPECTED_COMMAND)
            .isEqualTo(statement.toString());
    }
}
