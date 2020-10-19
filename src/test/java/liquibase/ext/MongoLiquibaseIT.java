package liquibase.ext;

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

import liquibase.Liquibase;
import liquibase.change.CheckSum;
import liquibase.exception.LiquibaseException;
import liquibase.ext.mongodb.changelog.MongoRanChangeSet;
import liquibase.ext.mongodb.changelog.MongoRanChangeSetToDocumentConverter;
import liquibase.ext.mongodb.statement.FindAllStatement;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class MongoLiquibaseIT extends AbstractMongoIntegrationTest {

    protected FindAllStatement findAll;
    protected MongoRanChangeSetToDocumentConverter converter = new MongoRanChangeSetToDocumentConverter();

    @BeforeEach
    protected void setUpEach() {
        super.setUpEach();
        findAll = new FindAllStatement(database.getDatabaseChangeLogTableName());
    }

    protected Integer countCollections() {
        List<Object> list = new ArrayList<>();
        connection.getDatabase().listCollectionNames().into(list);
        return list.size();
    }

    // TODO: Check something
    @Test
    void testMongoLiquibase() throws LiquibaseException {
        Liquibase liquiBase = new Liquibase("liquibase/ext/changelog.insert-one.test.xml", new ClassLoaderResourceAccessor(), database);
        liquiBase.update("");
    }

    @SneakyThrows
    @Test
    void testMongoClearChecksums() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.insert-one.test.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        List<MongoRanChangeSet> changeSets = findAll.queryForList(connection).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(3)
        .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getLastCheckSum)
        .containsExactly(
                tuple("1", 1, CheckSum.parse("8:4e072f0d1a237e4e98b5edac60c3f335")),
                tuple("2", 2, CheckSum.parse("8:e504f1757d0460c82b54b702794b8cf7")),
                tuple("3", 3, CheckSum.parse("8:4eff4f9e1b017ccce8da57f3c8125f13")));

        // Clear checksums
        liquibase.clearCheckSums();
        changeSets = findAll.queryForList(connection).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(3)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getLastCheckSum)
                .containsExactly(
                        tuple("1", 1, null),
                        tuple("2", 2, null),
                        tuple("3", 3, null));

        // Replace null checkSums
        liquibase.update("");

        changeSets = findAll.queryForList(connection).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(3)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getLastCheckSum)
                .containsExactly(
                        tuple("1", 1, CheckSum.parse("8:4e072f0d1a237e4e98b5edac60c3f335")),
                        tuple("2", 2, CheckSum.parse("8:e504f1757d0460c82b54b702794b8cf7")),
                        tuple("3", 3, CheckSum.parse("8:4eff4f9e1b017ccce8da57f3c8125f13")));
    }

    @Test
    void testMongoLiquibaseDropAll() throws LiquibaseException {
        Liquibase liquibase = new Liquibase("liquibase/ext/changelog.insert-one.test.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");
        assertThat(countCollections()).isEqualTo(5);
        liquibase.dropAll();
        assertThat(countCollections()).isEqualTo(0);

    }

}
