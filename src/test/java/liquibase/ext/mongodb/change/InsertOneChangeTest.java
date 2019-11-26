package liquibase.ext.mongodb.change;

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

import liquibase.changelog.ChangeSet;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.List;

import static liquibase.ext.mongodb.TestUtils.getChangesets;
import static org.assertj.core.api.Assertions.assertThat;

class InsertOneChangeTest extends AbstractMongoChangeTest {

    @Test
    void getConfirmationMessage() {
        assertThat(new InsertOneChange().getConfirmationMessage()).isNull();
    }

    @Test
    @SneakyThrows
    void generateStatements() {
        final List<ChangeSet> changeSets = getChangesets("liquibase/ext/changelog.insert-one.test.xml", database);

        assertThat(changeSets).hasSize(3);
        assertThat(changeSets.get(0).getChanges())
            .hasSize(1)
            .hasOnlyElementsOfType(InsertOneChange.class);
        assertThat(changeSets.get(1).getChanges())
            .hasSize(2)
            .hasOnlyElementsOfType(InsertOneChange.class);
        assertThat(changeSets.get(2).getChanges())
            .hasSize(2)
            .hasOnlyElementsOfType(InsertOneChange.class);

        assertThat(changeSets.get(0).getChanges().get(0))
            .hasFieldOrPropertyWithValue("collectionName", "insertOneTest1")
            .hasFieldOrPropertyWithValue("document", "{\n                id: 111\n                }")
            .hasFieldOrPropertyWithValue("options", null);

        assertThat(changeSets.get(1).getChanges().get(0))
            .hasFieldOrPropertyWithValue("collectionName", "insertOneTest2")
            .hasFieldOrPropertyWithValue("document", "{\n                id: 2\n                }")
            .hasFieldOrPropertyWithValue("options", null);

        assertThat(changeSets.get(1).getChanges().get(1))
            .hasFieldOrPropertyWithValue("collectionName", "insertOneTest3")
            .hasFieldOrPropertyWithValue("document", "{\n                id: 3\n                }")
            .hasFieldOrPropertyWithValue("options", null);

        assertThat(changeSets.get(2).getChanges().get(0))
            .hasFieldOrPropertyWithValue("collectionName", "insertOneTest2")
            .hasFieldOrPropertyWithValue("document", "{\n                id: 21323123\n                }")
            .hasFieldOrPropertyWithValue("options", null);

        assertThat(changeSets.get(2).getChanges().get(1))
            .hasFieldOrPropertyWithValue("collectionName", "insertOneTest3")
            .hasFieldOrPropertyWithValue("document", "{\n                id: 321321313\n                }")
            .hasFieldOrPropertyWithValue("options", null);
    }
}
