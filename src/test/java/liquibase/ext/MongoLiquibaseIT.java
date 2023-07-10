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
import liquibase.exception.CommandExecutionException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.mongodb.changelog.MongoRanChangeSet;
import liquibase.ext.mongodb.changelog.MongoRanChangeSetToDocumentConverter;
import liquibase.ext.mongodb.statement.FindAllStatement;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.SneakyThrows;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static liquibase.changelog.ChangeSet.ExecType.EXECUTED;
import static liquibase.changelog.ChangeSet.ExecType.SKIPPED;
import static liquibase.ext.mongodb.TestUtils.getCollections;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.tuple;

class MongoLiquibaseIT extends AbstractMongoIntegrationTest {

    protected FindAllStatement findAllRanChangeSets;
    protected MongoRanChangeSetToDocumentConverter converter = new MongoRanChangeSetToDocumentConverter();

    @BeforeEach
    protected void setUpEach() {
        super.setUpEach();
        findAllRanChangeSets = new FindAllStatement(database.getDatabaseChangeLogTableName());
    }

    protected Integer countCollections() {
        List<Object> list = new ArrayList<>();
        connection.getMongoDatabase().listCollectionNames().into(list);
        return list.size();
    }

    // TODO: Check something
    @Test
    void testLiquibase() throws LiquibaseException {
        Liquibase liquiBase = new Liquibase("liquibase/ext/changelog.insert-one.test.xml", new ClassLoaderResourceAccessor(), database);
        liquiBase.update("");
    }

    @SneakyThrows
    @Test
    void testClearChecksums() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.insert-one.test.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(3)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getLastCheckSum)
                .containsExactly(
                        tuple("1", 1, CheckSum.parse("9:66f74bbe4c1ae2aeec30a60885135611")),
                        tuple("2", 2, CheckSum.parse("9:8da4d127bd90a85116dfd6109a527ab2")),
                        tuple("3", 3, CheckSum.parse("9:ab691099a5db5a4ec05af5a310af1c40")));

        // Clear checksums
        liquibase.clearCheckSums();
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(3)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getLastCheckSum)
                .containsExactly(
                        tuple("1", 1, null),
                        tuple("2", 2, null),
                        tuple("3", 3, null));

        // Replace null checkSums
        liquibase.update("");

        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(3)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getLastCheckSum)
                .containsExactly(
                        tuple("1", 1, CheckSum.parse("9:66f74bbe4c1ae2aeec30a60885135611")),
                        tuple("2", 2, CheckSum.parse("9:8da4d127bd90a85116dfd6109a527ab2")),
                        tuple("3", 3, CheckSum.parse("9:ab691099a5db5a4ec05af5a310af1c40")));
    }

    @SneakyThrows
    @Test
    void testRollback() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.rollback-insert-many.test.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(2)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted)
                .containsExactly(
                        tuple("1", 1),
                        tuple("2", 2));

        FindAllStatement findAllInsertedRowsStatement = new FindAllStatement("insertManyRollback1");
        List<Document> insertedRows = findAllInsertedRowsStatement.queryForList(database);
        assertThat(insertedRows).hasSize(6)
                .extracting(d -> d.getInteger("id"))
                .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);

        // Rollback one changeSet
        liquibase.rollback(1, "");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(1)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted)
                .containsExactly(
                        tuple("1", 1));

        insertedRows = findAllInsertedRowsStatement.queryForList(database);
        assertThat(insertedRows).hasSize(3)
                .extracting(d -> d.getInteger("id"))
                .containsExactlyInAnyOrder(1, 2, 3);

        // Rollback last changeSet
        liquibase.rollback(1, "");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).isEmpty();

        insertedRows = findAllInsertedRowsStatement.queryForList(database);
        assertThat(insertedRows).isEmpty();

        // Re apply
        liquibase.update("");

        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(2)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted)
                .containsExactly(
                        tuple("1", 1),
                        tuple("2", 2));

        insertedRows = findAllInsertedRowsStatement.queryForList(database);
        assertThat(insertedRows).hasSize(6)
                .extracting(d -> d.getInteger("id"))
                .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);

        // Rollback both changeSet
        liquibase.rollback(2, "");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).isEmpty();

        insertedRows = findAllInsertedRowsStatement.queryForList(database);
        assertThat(insertedRows).isEmpty();

    }

    @SneakyThrows
    @Test
    void testImplicitRollback() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.implicit-rollback.test.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(2)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted)
                .containsExactly(
                        tuple("1", 1),
                        tuple("2", 2));

        assertThat(getCollections(connection))
                .hasSize(4)
                .containsExactlyInAnyOrder("DATABASECHANGELOG", "DATABASECHANGELOGLOCK", "collection1", "collection2");

        // Rollback one changeSet
        liquibase.rollback(1, "");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(1)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted)
                .containsExactly(
                        tuple("1", 1));

        assertThat(getCollections(connection))
                .hasSize(4)
                .containsExactlyInAnyOrder("DATABASECHANGELOG", "DATABASECHANGELOGLOCK", "collection1", "collection2");

        // Rollback last changeSet
        liquibase.rollback(1, "");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).isEmpty();

        assertThat(getCollections(connection))
                .hasSize(2)
                .containsExactlyInAnyOrder("DATABASECHANGELOG", "DATABASECHANGELOGLOCK");

        // Re apply
        liquibase.update("");

        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(2)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted)
                .containsExactly(
                        tuple("1", 1),
                        tuple("2", 2));

        assertThat(getCollections(connection))
                .hasSize(4)
                .containsExactlyInAnyOrder("DATABASECHANGELOG", "DATABASECHANGELOGLOCK", "collection1", "collection2");

        // Rollback both changeSet
        liquibase.rollback(2, "");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).isEmpty();

        assertThat(getCollections(connection))
                .hasSize(2)
                .containsExactlyInAnyOrder("DATABASECHANGELOG", "DATABASECHANGELOGLOCK");

    }


    @Test
    void testLiquibaseDropAll() throws LiquibaseException {
        Liquibase liquibase = new Liquibase("liquibase/ext/changelog.insert-one.test.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");
        assertThat(countCollections()).isEqualTo(5);
        liquibase.dropAll();
        assertThat(countCollections()).isEqualTo(0);

    }

    @SneakyThrows
    @Test
    void testPreconditions() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.insert-precondition.test.xml", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(10)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType, MongoRanChangeSet::getLastCheckSum)
                .containsExactly(
                        tuple("1", 1, SKIPPED, CheckSum.parse("9:dad21c5e3cce72d50076657128081c0c")),
                        tuple("2", 2, EXECUTED, CheckSum.parse("9:67e9ce155c04ef70eb553ff6cab13f58")),
                        tuple("3", 3, EXECUTED, CheckSum.parse("9:9bf4925aac3718c3eb1929e95b5332c9")),
                        tuple("4", 4, SKIPPED, CheckSum.parse("9:d37e34334465cfae484bbe8e3e6f1482")),
                        tuple("5", 5, EXECUTED, CheckSum.parse("9:b8ef52e2f9d7b96664577c026bc079e4")),
                        tuple("6", 6, EXECUTED, CheckSum.parse("9:2e1e5f512aaab73117ca23108ec8076f")),
                        tuple("7", 7, SKIPPED, CheckSum.parse("9:5e1fca21679293232bbade890e057bca")),
                        tuple("8", 8, EXECUTED, CheckSum.parse("9:b95cec9546c91ae40c4ce63f7901ad24")),
                        tuple("9", 9, EXECUTED, CheckSum.parse("9:ae8941a239134f17455b01f0d92ff53b")),
                        tuple("10", 10, SKIPPED, CheckSum.parse("9:89b8c6b5f63ad239e2993c84d24ad342"))
                );

        assertThat(getCollections(connection))
                .hasSize(4)
                .containsExactlyInAnyOrder("DATABASECHANGELOG", "DATABASECHANGELOGLOCK", "collection1", "results");

        final FindAllStatement findAllResults = new FindAllStatement("results");
        assertThat(findAllResults.queryForList(database))
                .hasSize(5).extracting(d -> d.get("info"))
                .containsExactlyInAnyOrder(
                        "existsAnyDocumentInCollection1",
                        "filterMatchedInCollection1",
                        "changeSetExecutedMatch",
                        "expectedDocumentCountFilterMatchedInCollection1",
                        "expectedCollectionResultsExists"
                );

    }

    @SneakyThrows
    @Test
    void testTags() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.insert-tags.test.xml", new ClassLoaderResourceAccessor(), database);

        // tag on an empty DB
        liquibase.tag("tag0");

        List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(1)
                .extracting(MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType, MongoRanChangeSet::getTag)
                .containsExactly(
                        tuple(1, EXECUTED, "tag0")
                );

        final FindAllStatement findAllResults = new FindAllStatement("results");
        assertThat(findAllResults.queryForList(database))
                .hasSize(0);

        // update to tag
        liquibase.dropAll();
        liquibase.update("tag5", "");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(5)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType, MongoRanChangeSet::getTag)
                .containsExactly(
                        tuple("1", 1, EXECUTED, null),
                        tuple("2", 2, EXECUTED, "tag2"),
                        tuple("3", 3, EXECUTED, null),
                        tuple("4", 4, EXECUTED, null),
                        tuple("5", 5, EXECUTED, "tag5")
                );

        assertThat(findAllResults.queryForList(database))
                .hasSize(3).extracting(d -> d.get("info"))
                .containsExactlyInAnyOrder("row1", "row3", "row4");

        liquibase.update("");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(6)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType, MongoRanChangeSet::getTag)
                .containsExactly(
                        tuple("1", 1, EXECUTED, null),
                        tuple("2", 2, EXECUTED, "tag2"),
                        tuple("3", 3, EXECUTED, null),
                        tuple("4", 4, EXECUTED, null),
                        tuple("5", 5, EXECUTED, "tag5"),
                        tuple("6", 6, EXECUTED, null)
                );
        assertThat(findAllResults.queryForList(database))
                .hasSize(4).extracting(d -> d.get("info"))
                .containsExactlyInAnyOrder("row1", "row3", "row4", "row6");

        // tag current state
        liquibase.tag("tag6");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(6)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType, MongoRanChangeSet::getTag)
                .containsExactly(
                        tuple("1", 1, EXECUTED, null),
                        tuple("2", 2, EXECUTED, "tag2"),
                        tuple("3", 3, EXECUTED, null),
                        tuple("4", 4, EXECUTED, null),
                        tuple("5", 5, EXECUTED, "tag5"),
                        tuple("6", 6, EXECUTED, "tag6")
                );
        assertThat(findAllResults.queryForList(database))
                .hasSize(4).extracting(d -> d.get("info"))
                .containsExactlyInAnyOrder("row1", "row3", "row4", "row6");

        // re tag current state
        liquibase.tag("retag6");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(6)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType, MongoRanChangeSet::getTag)
                .containsExactly(
                        tuple("1", 1, EXECUTED, null),
                        tuple("2", 2, EXECUTED, "tag2"),
                        tuple("3", 3, EXECUTED, null),
                        tuple("4", 4, EXECUTED, null),
                        tuple("5", 5, EXECUTED, "tag5"),
                        tuple("6", 6, EXECUTED, "retag6")
                );
        assertThat(findAllResults.queryForList(database))
                .hasSize(4).extracting(d -> d.get("info"))
                .containsExactlyInAnyOrder("row1", "row3", "row4", "row6");

        assertThat(getCollections(connection))
                .hasSize(3)
                .containsExactlyInAnyOrder("DATABASECHANGELOG", "DATABASECHANGELOGLOCK", "results");

        // rollback to not existing
        assertThatExceptionOfType(CommandExecutionException.class).isThrownBy(() -> liquibase.rollback("notExisting", ""))
                .withMessageContaining("Could not find tag 'notExisting' in the database");

        // rollback to tagged state. Tagged ChangeSet remains
        liquibase.rollback("retag6", "");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(6)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType, MongoRanChangeSet::getTag)
                .containsExactly(
                        tuple("1", 1, EXECUTED, null),
                        tuple("2", 2, EXECUTED, "tag2"),
                        tuple("3", 3, EXECUTED, null),
                        tuple("4", 4, EXECUTED, null),
                        tuple("5", 5, EXECUTED, "tag5"),
                        tuple("6", 6, EXECUTED, "retag6")
                );
        assertThat(findAllResults.queryForList(database))
                .hasSize(4).extracting(d -> d.get("info"))
                .containsExactlyInAnyOrder("row1", "row3", "row4", "row6");
        assertThat(liquibase.tagExists("retag6")).isTrue();

        // rollback to tag2 tag ChangeSet is removed
        assertThat(liquibase.tagExists("tag2")).isTrue();
        liquibase.rollback("tag2", "");
        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(1)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType, MongoRanChangeSet::getTag)
                .containsExactly(
                        tuple("1", 1, EXECUTED, null)
                );
        assertThat(findAllResults.queryForList(database))
                .hasSize(1).extracting(d -> d.get("info"))
                .containsExactlyInAnyOrder("row1");

        assertThat(liquibase.tagExists("tag2")).isFalse();
    }

}
