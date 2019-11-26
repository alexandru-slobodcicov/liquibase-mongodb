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

import liquibase.change.Change;
import liquibase.changelog.ChangeSet;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.List;

import static liquibase.ext.mongodb.TestUtils.getChangesets;
import static org.assertj.core.api.Assertions.assertThat;

class CreateIndexChangeTest extends AbstractMongoChangeTest {

    @Test
    void getConfirmationMessage() {
        assertThat(new CreateIndexChange().getConfirmationMessage()).isNull();
    }

    @Test
    @SneakyThrows
    void generateStatements() {
        final List<ChangeSet> changeSets = getChangesets("liquibase/ext/changelog.create-index.test.xml", database);

        assertThat(changeSets).hasSize(1);

        final List<Change> changes1 = changeSets.get(0).getChanges();
        assertThat(changes1).hasSize(2);
        assertThat(changes1).hasOnlyElementsOfType(CreateIndexChange.class);

        assertThat(changes1.get(0))
            .hasFieldOrPropertyWithValue("CollectionName", "createIndexTest")
            .hasFieldOrPropertyWithValue("keys", "{ clientId: 1, type: 1}")
            .hasFieldOrPropertyWithValue("options", "{unique: true, name: \"ui_tppClientId\"}");

        assertThat(changes1.get(1))
            .hasFieldOrPropertyWithValue("CollectionName", "createIndexNoOptionsTest")
            .hasFieldOrPropertyWithValue("keys", "{ clientId: 1, type: 1}")
            .hasFieldOrPropertyWithValue("options", null);
    }
}
