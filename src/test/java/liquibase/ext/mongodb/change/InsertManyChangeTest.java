package liquibase.ext.mongodb.change;

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

import liquibase.ChecksumVersion;
import liquibase.changelog.ChangeSet;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.List;

import static liquibase.ext.mongodb.TestUtils.getChangesets;
import static org.assertj.core.api.Assertions.assertThat;

class InsertManyChangeTest extends AbstractMongoChangeTest {

    @Test
    void getConfirmationMessage() {
        final InsertManyChange insertManyChange = new InsertManyChange();
        insertManyChange.setCollectionName("collection1");
        assertThat(insertManyChange.getConfirmationMessage()).isEqualTo("Documents inserted into collection collection1");
    }

    @Test
    @SneakyThrows
    void generateStatements() {
        final List<ChangeSet> changeSets = getChangesets("liquibase/ext/changelog.insert-many.test.xml", database);

        assertThat(changeSets)
                .hasSize(1).first()
                .returns("9:ae462af55d2b62a1c0898f356c614249",  changeSet -> changeSet.generateCheckSum(ChecksumVersion.latest()).toString());

        assertThat(changeSets.get(0).getChanges())
                .hasSize(1)
                .hasOnlyElementsOfType(InsertManyChange.class);

        assertThat(changeSets.get(0).getChanges().get(0))
                .hasFieldOrPropertyWithValue("collectionName", "insertManyTest1")
                .hasFieldOrPropertyWithValue("documents", "[\n                { id: 2 },\n                { id: 3,\n                  "
                        + "address: { nr: 1, ap: 5}\n                }\n                ]")
                .hasFieldOrPropertyWithValue("options", null);
    }
}
