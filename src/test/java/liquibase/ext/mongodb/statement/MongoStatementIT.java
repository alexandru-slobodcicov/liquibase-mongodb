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

import liquibase.change.CheckSum;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.RanChangeSet;
import liquibase.exception.LiquibaseException;
import liquibase.ext.AbstractMongoIntegrationTest;
import liquibase.ext.mongodb.changelog.ChangeSetUtils;
import liquibase.ext.mongodb.changelog.CreateChangeLogCollectionStatement;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

//TODO: Add Unit Test with mocks

class MongoStatementIT extends AbstractMongoIntegrationTest {

    @Test
    void testInsertOneStatement() throws LiquibaseException {

        final String collectionName = "logCollection";
        new CreateChangeLogCollectionStatement(collectionName).execute(database.getConnection().getDb());

        Date expectedDateExecuted = new Date();

        RanChangeSet ranChangeSet = new RanChangeSet(
                "fileName"
                , "1"
                , "author"
                , CheckSum.compute("md5sum")
                , expectedDateExecuted
                , "tag"
                , ChangeSet.ExecType.EXECUTED
                , "description"
                , "comments"
                , null
                , null
                , "liquibase"
        );
        ranChangeSet.setOrderExecuted(2);
        Document document = ChangeSetUtils.toDocument(ranChangeSet);

        new InsertOneStatement(collectionName,document , null).execute(database.getConnection().getDb());
        assertThat(database.getConnection().getDb().getCollection(collectionName).countDocuments()).isEqualTo(1L);

        document.put("id", "2");

        ranChangeSet = ChangeSetUtils.fromDocument(document);

        ranChangeSet.setOrderExecuted(1);
        document = ChangeSetUtils.toDocument(ranChangeSet);

        new InsertOneStatement(collectionName,document , null).execute(database.getConnection().getDb());
        assertThat(database.getConnection().getDb().getCollection(collectionName).countDocuments()).isEqualTo(2L);

        final FindAllStatement findAllStatement = new FindAllStatement(collectionName);

        final List ranChangeSetList = findAllStatement.queryForList(database.getConnection().getDb(), Document.class);

        assertThat(ranChangeSetList.size()).isEqualTo(2);

    }

    @Test
    void testInsertOneChange() throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
                ChangeLogParserFactory.getInstance().getParser("xml", resourceAccessor);

        final DatabaseChangeLog changeLog = parser.parse("liquibase/ext/changelog.insert-one.test.xml", new ChangeLogParameters(database), resourceAccessor);

        changeLog.getChangeSets()
                .stream()
                .flatMap(cs -> cs.getChanges().stream())
                .flatMap(c -> Stream.of(c.generateStatements(database)))
                .forEach(mongoExecutor::execute);

        assertThat(
                mongoConnection
                        .getDb()
                        .getCollection("insertOneTest1")
                    .countDocuments()).isEqualTo(1L);

        assertThat(
                mongoConnection
                        .getDb()
                        .getCollection("insertOneTest2")
                    .countDocuments()).isEqualTo(2L);

    }


    @Test
    void testInsertManyChange() throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
                ChangeLogParserFactory.getInstance().getParser("xml", resourceAccessor);

        final DatabaseChangeLog changeLog = parser.parse("liquibase/ext/changelog.insert-many.test.xml", new ChangeLogParameters(database), resourceAccessor);
        changeLog.getChangeSets()
                .stream()
                .flatMap(cs -> cs.getChanges().stream())
                .flatMap(c -> Stream.of(c.generateStatements(database)))
                .forEach(mongoExecutor::execute);

        assertThat(
                mongoConnection
                        .getDb()
                        .getCollection("insertManyTest1")
                    .countDocuments()).isEqualTo(2L);
    }


    @Test
    void testCreateCollectionChange() throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
                ChangeLogParserFactory.getInstance().getParser("xml", resourceAccessor);

        final DatabaseChangeLog
                changeLog =
                parser.parse("liquibase/ext/changelog.create-collection.test.xml", new ChangeLogParameters(database), resourceAccessor);
        changeLog.getChangeSets()
                .stream()
                .flatMap(cs -> cs.getChanges().stream())
                .flatMap(c -> Stream.of(c.generateStatements(database)))
                .forEach(mongoExecutor::execute);

        assertThat(
                mongoConnection
                        .getDb()
                    .getCollection("createCollectionTest")).isNotNull();
    }

    @Test
    void testCreateIndexChange() throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
                ChangeLogParserFactory.getInstance().getParser("xml", resourceAccessor);

        final DatabaseChangeLog
                changeLog =
                parser.parse("liquibase/ext/changelog.create-index.test.xml", new ChangeLogParameters(database), resourceAccessor);
        changeLog.getChangeSets()
                .stream()
                .flatMap(cs -> cs.getChanges().stream())
                .flatMap(c -> Stream.of(c.generateStatements(database)))
                .forEach(mongoExecutor::execute);
    }

}
