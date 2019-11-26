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
import liquibase.ext.mongodb.statement.AdminCommandStatement;
import liquibase.statement.SqlStatement;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.List;

import static liquibase.ext.mongodb.TestUtils.BUILD_INFO_1;
import static liquibase.ext.mongodb.TestUtils.getChangesets;
import static org.assertj.core.api.Assertions.assertThat;

class AdminCommandChangeTest extends AbstractMongoChangeTest {

    @Test
    void getConfirmationMessage() {
        assertThat(new AdminCommandChange().getConfirmationMessage()).isNull();
    }

    @Test
    @SneakyThrows
    void generateStatements() {
        final List<ChangeSet> changeSets = getChangesets("liquibase/ext/changelog.admin-command.test.xml", database);

        assertThat(changeSets).hasSize(1);

        assertThat(changeSets.get(0).getChanges())
            .hasSize(1)
            .hasOnlyElementsOfTypes(AdminCommandChange.class);

        final AdminCommandChange ch2 = (AdminCommandChange) changeSets.get(0).getChanges().get(0);
        assertThat(ch2.getCommand()).isEqualTo(BUILD_INFO_1);

        final SqlStatement[] sqlStatements2 = ch2.generateStatements(database);
        assertThat(sqlStatements2).hasSize(1);
        assertThat(sqlStatements2).hasOnlyElementsOfType(AdminCommandStatement.class);
        assertThat(((AdminCommandStatement) sqlStatements2[0]).getCommand()).containsEntry("buildInfo", 1);
    }
}
