package liquibase.ext.mongodb.statement;

import liquibase.change.CheckSum;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.RanChangeSet;
import liquibase.exception.DatabaseException;
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

class MongoStatementIntegrationTest extends AbstractMongoIntegrationTest {

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

        ranChangeSet = new RanChangeSet(
                "fileName"
                , "2"
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
                .forEach(stmt -> {
                    try {
                        mongoExecutor.execute(stmt);
                    } catch (DatabaseException e) {
                        e.printStackTrace();
                    }
                });

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
                .forEach(stmt -> {
                    try {
                        mongoExecutor.execute(stmt);
                    } catch (DatabaseException e) {
                        e.printStackTrace();
                    }
                });

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
                .forEach(stmt -> {
                    try {
                        mongoExecutor.execute(stmt);
                    } catch (DatabaseException e) {
                        e.printStackTrace();
                    }
                });

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
                .forEach(stmt -> {
                    try {
                        mongoExecutor.execute(stmt);
                    } catch (DatabaseException e) {
                        e.printStackTrace();
                    }
                });
    }

}
