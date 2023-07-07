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
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import liquibase.exception.CommandExecutionException;
import liquibase.ext.mongodb.changelog.MongoRanChangeSet;
import liquibase.ext.mongodb.changelog.MongoRanChangeSetToDocumentConverter;
import liquibase.ext.mongodb.statement.FindAllStatement;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.util.LiquibaseUtil;
import lombok.SneakyThrows;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;
import static liquibase.changelog.ChangeSet.ExecType.EXECUTED;
import static liquibase.changelog.ChangeSet.ExecType.SKIPPED;
import static liquibase.ext.mongodb.TestUtils.getCollections;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.tuple;

class MongoLiquibaseJsonIT extends AbstractMongoIntegrationTest {

    protected FindAllStatement findAllRanChangeSets;
    protected MongoRanChangeSetToDocumentConverter converter = new MongoRanChangeSetToDocumentConverter();

    @BeforeEach
    protected void setUpEach() {
        super.setUpEach();
        findAllRanChangeSets = new FindAllStatement(database.getDatabaseChangeLogTableName());
    }

    @SneakyThrows
    @Test
    void testLiquibase() {
        Liquibase liquiBase = new Liquibase("liquibase/ext/json/generic-1-insert-people.json", new ClassLoaderResourceAccessor(), database);
        liquiBase.update("");

        final List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(2)
                .filteredOn(c -> c.getId().equals("1")).hasSize(1).first()
                .returns("liquibase/ext/json/generic-1-insert-people.json", RanChangeSet::getChangeLog)
                .returns("Alex", RanChangeSet::getAuthor)
                .returns(CheckSum.parse("9:95727cf24d16eda63ee7e0ca62f9e713"), RanChangeSet::getLastCheckSum)
                .returns(true, c -> nonNull(c.getDateExecuted()))
                .returns(null, RanChangeSet::getTag)
                .returns(ChangeSet.ExecType.EXECUTED, RanChangeSet::getExecType)
                .returns("createCollection collectionName=person", RanChangeSet::getDescription)
                .returns("Create person collection", RanChangeSet::getComments)
                .returns(true, c -> c.getContextExpression().isEmpty())
                .returns(true, c -> c.getLabels().isEmpty())
                .returns(true, c -> c.getDeploymentId().matches("^[0-9]{10}$"))
                .returns(1, RanChangeSet::getOrderExecuted)
                .returns(LiquibaseUtil.getBuildVersion(), RanChangeSet::getLiquibaseVersion);

        assertThat(changeSets)
                .filteredOn(c -> c.getId().equals("2")).hasSize(1).first()
                .returns("liquibase/ext/json/generic-1-insert-people.json", RanChangeSet::getChangeLog)
                .returns("Nick", RanChangeSet::getAuthor)
                .returns(CheckSum.parse("9:9eb024092590f9c802032847573771af"), RanChangeSet::getLastCheckSum)
                .returns(true, c -> nonNull(c.getDateExecuted()))
                .returns(null, RanChangeSet::getTag)
                .returns(ChangeSet.ExecType.EXECUTED, RanChangeSet::getExecType)
                .returns("insertOne collectionName=person; insertMany collectionName=person", RanChangeSet::getDescription)
                .returns("Populate person table", RanChangeSet::getComments)
                .returns(true, c -> c.getContextExpression().isEmpty())
                .returns(true, c -> c.getLabels().isEmpty())
                .returns(true, c -> c.getDeploymentId().matches("^[0-9]{10}$"))
                .returns(2, RanChangeSet::getOrderExecuted)
                .returns(LiquibaseUtil.getBuildVersion(), RanChangeSet::getLiquibaseVersion);
    }

    @SneakyThrows
    @Test
    void testParentFile() {
        Liquibase liquiBase = new Liquibase("liquibase/ext/json/generic-0-main-1.json", new ClassLoaderResourceAccessor(), database);
        liquiBase.update("");

        final List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(2)
                .filteredOn(c -> c.getId().equals("1")).hasSize(1).first()
                .returns("liquibase/ext/json/generic-1-insert-people.json", RanChangeSet::getChangeLog)
                .returns("Alex", RanChangeSet::getAuthor)
                .returns(CheckSum.parse("9:95727cf24d16eda63ee7e0ca62f9e713"), RanChangeSet::getLastCheckSum)
                .returns(true, c -> nonNull(c.getDateExecuted()))
                .returns(null, RanChangeSet::getTag)
                .returns(ChangeSet.ExecType.EXECUTED, RanChangeSet::getExecType)
                .returns("createCollection collectionName=person", RanChangeSet::getDescription)
                .returns("Create person collection", RanChangeSet::getComments)
                .returns(true, c -> c.getContextExpression().isEmpty())
                .returns(true, c -> c.getLabels().isEmpty())
                .returns(true, c -> c.getDeploymentId().matches("^[0-9]{10}$"))
                .returns(1, RanChangeSet::getOrderExecuted)
                .returns(LiquibaseUtil.getBuildVersion(), RanChangeSet::getLiquibaseVersion);

        assertThat(changeSets)
                .filteredOn(c -> c.getId().equals("2")).hasSize(1).first()
                .returns("liquibase/ext/json/generic-1-insert-people.json", RanChangeSet::getChangeLog)
                .returns("Nick", RanChangeSet::getAuthor)
                .returns(CheckSum.parse("9:9eb024092590f9c802032847573771af"), RanChangeSet::getLastCheckSum)
                .returns(true, c -> nonNull(c.getDateExecuted()))
                .returns(null, RanChangeSet::getTag)
                .returns(ChangeSet.ExecType.EXECUTED, RanChangeSet::getExecType)
                .returns("insertOne collectionName=person; insertMany collectionName=person", RanChangeSet::getDescription)
                .returns("Populate person table", RanChangeSet::getComments)
                .returns(true, c -> c.getContextExpression().isEmpty())
                .returns(true, c -> c.getLabels().isEmpty())
                .returns(true, c -> c.getDeploymentId().matches("^[0-9]{10}$"))
                .returns(2, RanChangeSet::getOrderExecuted)
                .returns(LiquibaseUtil.getBuildVersion(), RanChangeSet::getLiquibaseVersion);
    }

    @SneakyThrows
    @Test
    void testLiquibaseIncremental() {
        Liquibase liquibase = new Liquibase("liquibase/ext/json/generic-0-main-1.json", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(2)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getLastCheckSum)
                .containsExactly(
                        tuple("1", 1, CheckSum.parse("9:95727cf24d16eda63ee7e0ca62f9e713")),
                        tuple("2", 2, CheckSum.parse("9:9eb024092590f9c802032847573771af")));

        List<Document> documents = new FindAllStatement("person").queryForList(database);
        assertThat(documents).hasSize(3)
                .extracting(d -> d.get("name"), d -> d.get("address"), d -> d.get("age"))
                .containsExactlyInAnyOrder(
                        tuple("Alexandru Slobodcicov", "Moldova", null),
                        tuple("Nicolas Bodros", "Spain", 34),
                        tuple("Luka Modrich", null, 55));


        liquibase = new Liquibase("liquibase/ext/json/generic-0-main-2.json", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(4)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getLastCheckSum)
                .containsExactly(
                        tuple("1", 1, CheckSum.parse("9:95727cf24d16eda63ee7e0ca62f9e713")),
                        tuple("2", 2, CheckSum.parse("9:9eb024092590f9c802032847573771af")),
                        tuple("1", 3, CheckSum.parse("9:bfb744c23f97ee4bd9df050d189efa08")),
                        tuple("2", 4, CheckSum.parse("9:ac7ea4fec237f17c4ae15a7a5ab1c7f0")));

        documents = new FindAllStatement("person").queryForList(database);
        assertThat(documents).hasSize(2)
                .extracting(d -> d.get("name"), d -> d.get("address"), d -> d.get("age"))
                .containsExactlyInAnyOrder(
                        tuple("Nicolas Bodros", "Spain", 34),
                        tuple("Luka Modrich", "Lapland", 55));
    }

    @SneakyThrows
    @Test
    void testPreconditions() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/json/changelog.insert-precondition.test.json", new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(8)
                .extracting(MongoRanChangeSet::getId, MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType)
                .containsExactly(
                        tuple("1", 1, SKIPPED),
                        tuple("2", 2, EXECUTED),
                        tuple("3", 3, EXECUTED),
                        tuple("4", 4, SKIPPED),
                        tuple("5", 5, EXECUTED),
                        tuple("6", 6, EXECUTED),
                        tuple("7", 7, SKIPPED),
                        tuple("8", 8, EXECUTED)
                );

        assertThat(getCollections(connection))
                .hasSize(4)
                .containsExactlyInAnyOrder("DATABASECHANGELOG", "DATABASECHANGELOGLOCK", "collection1", "results");

        final FindAllStatement findAllResults = new FindAllStatement("results");
        assertThat(findAllResults.queryForList(database))
                .hasSize(4).extracting(d -> d.get("info"))
                .containsExactlyInAnyOrder("existsAnyDocumentInCollection1", "filterMatchedInCollection1", "changeSetExecutedMatch", "expectedDocumentCountFilterMatchedInCollection1");

    }

    @SneakyThrows
    @Test
    void testTags() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/json/changelog.insert-tags.json", new ClassLoaderResourceAccessor(), database);

        // tag on an empty DB
        liquibase.tag("tag0");

        List<MongoRanChangeSet> changeSets = findAllRanChangeSets.queryForList(database).stream().map(converter::fromDocument).collect(Collectors.toList());
        assertThat(changeSets).hasSize(1)
                .extracting(MongoRanChangeSet::getOrderExecuted, MongoRanChangeSet::getExecType, MongoRanChangeSet::getTag)
                .containsExactly(
                        tuple( 1, EXECUTED, "tag0")
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
        assertThatExceptionOfType(CommandExecutionException.class).isThrownBy(()-> liquibase.rollback("notExisting", ""))
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
