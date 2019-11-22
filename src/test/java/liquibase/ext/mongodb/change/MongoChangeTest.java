package liquibase.ext.mongodb.change;

import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.LiquibaseException;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.ext.mongodb.statement.AdminCommandStatement;
import liquibase.ext.mongodb.statement.CreateCollectionStatement;
import liquibase.ext.mongodb.statement.RunCommandStatement;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

class MongoChangeTest {

    private MongoLiquibaseDatabase database;

    @BeforeEach
    void setUp() {
        database = new MongoLiquibaseDatabase();
    }

    @Test
    void testInsertOneChange() throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
                ChangeLogParserFactory.getInstance().getParser("xml", resourceAccessor);

        final DatabaseChangeLog changeLog = parser.parse("liquibase/ext/changelog.insert-one.test.xml", new ChangeLogParameters(database), resourceAccessor);
        final List<ChangeSet> changeSets = changeLog.getChangeSets();

        assertThat(changeSets, notNullValue());
        assertThat(changeSets, hasSize(3));
        assertThat(changeSets.get(0).getChanges(), hasSize(1));
        assertThat(changeSets.get(0).getChanges().get(0), instanceOf(InsertOneChange.class));
        assertThat(((InsertOneChange) changeSets.get(0).getChanges().get(0)).getCollectionName(), equalTo("insertOneTest1"));
        assertThat(changeSets.get(1).getChanges(), hasSize(2));
        assertThat(changeSets.get(1).getChanges().get(0), instanceOf(InsertOneChange.class));
        assertThat(((InsertOneChange) changeSets.get(1).getChanges().get(0)).getCollectionName(), equalTo("insertOneTest2"));
        assertThat(changeSets.get(1).getChanges().get(1), instanceOf(InsertOneChange.class));
        assertThat(((InsertOneChange) changeSets.get(1).getChanges().get(1)).getCollectionName(), equalTo("insertOneTest3"));
        //TODO: check all fields are parsed
    }

    @Test
    void testInsertManyChange() throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
                ChangeLogParserFactory.getInstance().getParser("xml", resourceAccessor);

        final DatabaseChangeLog changeLog = parser.parse("liquibase/ext/changelog.insert-many.test.xml", new ChangeLogParameters(database), resourceAccessor);
        final List<ChangeSet> changeSets = changeLog.getChangeSets();

        assertThat(changeSets, notNullValue());
        assertThat(changeSets, hasSize(1));
        assertThat(changeSets.get(0).getChanges(), hasSize(1));
        assertThat(changeSets.get(0).getChanges().get(0), instanceOf(InsertManyChange.class));
        assertThat(((InsertManyChange) changeSets.get(0).getChanges().get(0)).getCollectionName(), equalTo("insertManyTest1"));
        //TODO: check all fields are parsed
    }


    @Test
    void testCreateCollection() throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
                ChangeLogParserFactory.getInstance().getParser("xml", resourceAccessor);

        final DatabaseChangeLog
                changeLog =
                parser.parse("liquibase/ext/changelog.create-collection.test.xml", new ChangeLogParameters(database), resourceAccessor);
        final List<ChangeSet> changeSets = changeLog.getChangeSets();

        assertThat(changeSets, notNullValue());
        assertThat(changeSets, hasSize(1));
        assertThat(changeSets.get(0).getChanges(), hasSize(3));
        CreateCollectionChange ch1 = (CreateCollectionChange) changeSets.get(0).getChanges().get(0);
        CreateCollectionChange ch2 = (CreateCollectionChange) changeSets.get(0).getChanges().get(1);
        CreateCollectionChange ch3 = (CreateCollectionChange) changeSets.get(0).getChanges().get(2);
        assertThat(ch1.getCollectionName(), equalTo("createCollectionWithValidatorAndOptionsTest"));
        assertThat(ch1.getOptions().trim().isEmpty(), equalTo(false));
        assertThat(Arrays.asList(ch1.generateStatements(new MongoLiquibaseDatabase())), hasSize(1));
        assertThat(((CreateCollectionStatement)(ch1.generateStatements(new MongoLiquibaseDatabase())[0])).getCollectionName(), equalTo("createCollectionWithValidatorAndOptionsTest"));
        assertThat(((CreateCollectionStatement)(ch1.generateStatements(new MongoLiquibaseDatabase())[0])).getOptions().isEmpty(), Matchers.is(false));
        assertThat(ch2.getCollectionName(), equalTo("createCollectionWithEmptyValidatorTest"));
        assertThat(Arrays.asList(ch2.generateStatements(new MongoLiquibaseDatabase())), hasSize(1));
        assertThat(((CreateCollectionStatement)(ch2.generateStatements(new MongoLiquibaseDatabase())[0])).getCollectionName(), equalTo("createCollectionWithEmptyValidatorTest"));
        assertThat(((CreateCollectionStatement)(ch2.generateStatements(new MongoLiquibaseDatabase())[0])).getOptions().isEmpty(), Matchers.is(true));
        assertThat(ch3.getCollectionName(), equalTo("createCollectionWithNoValidator"));
        assertThat(Arrays.asList(ch3.generateStatements(new MongoLiquibaseDatabase())), hasSize(1));
        assertThat(((CreateCollectionStatement)(ch3.generateStatements(new MongoLiquibaseDatabase())[0])).getCollectionName(), equalTo("createCollectionWithNoValidator"));
        assertThat(((CreateCollectionStatement)(ch3.generateStatements(new MongoLiquibaseDatabase())[0])).getOptions().isEmpty(), Matchers.is(true));
    }

    @Test
    void testCreateIndex() throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
                ChangeLogParserFactory.getInstance().getParser("xml", resourceAccessor);

        final DatabaseChangeLog
                changeLog =
                parser.parse("liquibase/ext/changelog.create-index.test.xml", new ChangeLogParameters(database), resourceAccessor);
        final List<ChangeSet> changeSets = changeLog.getChangeSets();

        assertThat(changeSets, notNullValue());
        assertThat(changeSets.size(), equalTo(1));
        assertThat(changeSets.get(0).getChanges(), hasSize(2));
        assertThat(changeSets.get(0).getChanges().get(0), instanceOf(CreateIndexChange.class));
        CreateIndexChange ch1 = (CreateIndexChange) changeSets.get(0).getChanges().get(0);
        assertThat(ch1.getCollectionName(), equalTo("createIndexTest"));
        assertThat(ch1.getKeys(), equalTo("{ clientId: 1, type: 1}"));
        assertThat(ch1.getOptions(), equalTo("{unique: true, name: \"ui_tppClientId\"}"));
        CreateIndexChange ch2 = (CreateIndexChange) changeSets.get(0).getChanges().get(1);
        assertThat(ch2.getCollectionName(), equalTo("createIndexNoOptionsTest"));
        assertThat(ch2.getKeys(), notNullValue());
        assertThat(ch2.getOptions(), nullValue());

        //TODO: check all fields are parsed
    }


    @Test
    void testRunCommand() throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
                ChangeLogParserFactory.getInstance().getParser("xml", resourceAccessor);

        final DatabaseChangeLog
                changeLog =
                parser.parse("liquibase/ext/changelog.run-command.test.xml", new ChangeLogParameters(database), resourceAccessor);
        final List<ChangeSet> changeSets = changeLog.getChangeSets();

        assertThat(changeSets, notNullValue());
        assertThat(changeSets, hasSize(2));
        assertThat(changeSets.get(0).getChanges(), hasSize(1));
        assertThat(changeSets.get(0).getChanges().get(0), instanceOf(RunCommandChange.class));
        RunCommandChange ch1 = (RunCommandChange) changeSets.get(0).getChanges().get(0);
        assertThat(ch1.getCommand(), equalTo("{ buildInfo: 1 }"));
        assertThat(ch1.generateStatements(new MongoLiquibaseDatabase()).length, equalTo(1));
        assertThat(ch1.generateStatements(new MongoLiquibaseDatabase())[0], instanceOf(RunCommandStatement.class));
        assertThat(((RunCommandStatement)(ch1.generateStatements(new MongoLiquibaseDatabase())[0])).getCommand().getInteger("buildInfo"), equalTo(1));

        assertThat(changeSets.get(1).getChanges(), hasSize(1));
        assertThat(changeSets.get(1).getChanges().get(0), instanceOf(AdminCommandChange.class));
        AdminCommandChange ch2 = (AdminCommandChange) changeSets.get(1).getChanges().get(0);
        assertThat(ch2.getCommand(), equalTo("{ buildInfo: 1 }"));
        assertThat(ch2.generateStatements(new MongoLiquibaseDatabase()).length, equalTo(1));
        assertThat(ch2.generateStatements(new MongoLiquibaseDatabase())[0], instanceOf(AdminCommandStatement.class));
        assertThat(((AdminCommandStatement)(ch2.generateStatements(new MongoLiquibaseDatabase())[0])).getCommand().getInteger("buildInfo"), equalTo(1));

    }
}