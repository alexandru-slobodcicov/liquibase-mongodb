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

import liquibase.changelog.ChangeSet;
import liquibase.ext.mongodb.statement.CreateCollectionStatement;
import liquibase.statement.SqlStatement;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.List;

import static liquibase.ext.mongodb.TestUtils.getChangesets;
import static org.assertj.core.api.Assertions.assertThat;

class CreateCollectionChangeTest extends AbstractMongoChangeTest {

    @Test
    void getConfirmationMessage() {
        assertThat(new CreateCollectionChange().getConfirmationMessage())
                .isNull();
    }

    @Test
    @SneakyThrows
    void generateStatements() {
        final List<ChangeSet> changeSets = getChangesets("liquibase/ext/changelog.create-collection.test.xml", database);

        assertThat(changeSets)
                .isNotNull()
                .hasSize(1);
        assertThat(changeSets.get(0).getChanges())
                .hasSize(3)
                .hasOnlyElementsOfType(CreateCollectionChange.class);

        final CreateCollectionChange ch1 = (CreateCollectionChange) changeSets.get(0).getChanges().get(0);
        assertThat(ch1.getCollectionName()).isEqualTo("createCollectionWithValidatorAndOptionsTest");
        assertThat(ch1.getOptions()).isNotBlank();
        final SqlStatement[] sqlStatement1 = ch1.generateStatements(database);
        assertThat(sqlStatement1)
                .hasSize(1);
        assertThat(((CreateCollectionStatement) sqlStatement1[0]))
                .hasNoNullFieldsOrProperties()
                .hasFieldOrPropertyWithValue("collectionName", "createCollectionWithValidatorAndOptionsTest");

        final CreateCollectionChange ch2 = (CreateCollectionChange) changeSets.get(0).getChanges().get(1);
        assertThat(ch2.getCollectionName()).isEqualTo("createCollectionWithEmptyValidatorTest");
        assertThat(ch2.getOptions()).isBlank();
        final SqlStatement[] sqlStatement2 = ch2.generateStatements(database);
        assertThat(sqlStatement2)
                .hasSize(1);
        assertThat(((CreateCollectionStatement) sqlStatement2[0]))
                .hasNoNullFieldsOrPropertiesExcept("options")
                .hasFieldOrPropertyWithValue("collectionName", "createCollectionWithEmptyValidatorTest");

        final CreateCollectionChange ch3 = (CreateCollectionChange) changeSets.get(0).getChanges().get(2);
        assertThat(ch3.getCollectionName()).isEqualTo("createCollectionWithNoValidator");
        assertThat(ch3.getOptions()).isBlank();
        final SqlStatement[] sqlStatement3 = ch3.generateStatements(database);
        assertThat(sqlStatement3)
                .hasSize(1);
        assertThat(((CreateCollectionStatement) sqlStatement3[0]))
                .hasNoNullFieldsOrPropertiesExcept("options")
                .hasFieldOrPropertyWithValue("collectionName", "createCollectionWithNoValidator");
    }

    @Test
    @SneakyThrows
    void generateStatementsFromJson() {
        final List<ChangeSet> changeSets = getChangesets("liquibase/ext/json/changelog.generic.json", database);

        assertThat(changeSets)
                .isNotNull()
                .hasSize(1);
        assertThat(changeSets.get(0).getChanges())
                .hasSize(2)
                .hasOnlyElementsOfType(CreateCollectionChange.class);

        final CreateCollectionChange ch1 = (CreateCollectionChange) changeSets.get(0).getChanges().get(0);
        assertThat(ch1.getCollectionName()).isEqualTo("person");
        assertThat(ch1.getOptions()).isNull();
        final SqlStatement[] sqlStatement1 = ch1.generateStatements(database);
        assertThat(sqlStatement1)
                .hasSize(1);
        assertThat(((CreateCollectionStatement) sqlStatement1[0]))
                .hasNoNullFieldsOrProperties()
                .hasFieldOrPropertyWithValue("collectionName", "person");

        final CreateCollectionChange ch2 = (CreateCollectionChange) changeSets.get(0).getChanges().get(1);
        assertThat(ch2.getCollectionName()).isEqualTo("person1");
        assertThat(ch2.getOptions()).isNotBlank();
        final SqlStatement[] sqlStatement2 = ch2.generateStatements(database);
        assertThat(sqlStatement2)
                .hasSize(1);
        assertThat(((CreateCollectionStatement) sqlStatement2[0]))
                .hasFieldOrPropertyWithValue("collectionName", "person1")
                .hasNoNullFieldsOrPropertiesExcept("options");
    }
}
