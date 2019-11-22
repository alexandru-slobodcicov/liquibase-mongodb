package liquibase.ext;

import liquibase.Liquibase;
import liquibase.command.CommandFactory;
import liquibase.exception.LiquibaseException;
import liquibase.ext.mongodb.TestUtils;
import liquibase.ext.mongodb.database.DropCollectionsCommand;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MongoLiquibaseIntegrationCreateUsersTest extends AbstractMongoIntegrationTest {

    @BeforeEach
    void advancedCleanUp() {
        cleanup();
    }

    @AfterAll
    private static void cleanup() {
        TestUtils.dropAllRoles(mongoConnection);
        TestUtils.dropAllUsers(mongoConnection);
    }

    @Test
    void testMongoLiquibaseCreateUsers() throws LiquibaseException {
        final Liquibase liquiBase = new Liquibase("liquibase/ext/changelog.create-users.test.xml", new ClassLoaderResourceAccessor(), database);
        TestUtils.commit(liquiBase);
        assertThat(TestUtils.getAllRoles(mongoConnection)).hasSize(4);
    }

    @Test
    void testMongoLiquibaseDropAllUsers() throws LiquibaseException {
        final Liquibase liquiBase = new Liquibase("liquibase/ext/changelog.drop-users.test.xml", new ClassLoaderResourceAccessor(), database);
        TestUtils.commit(liquiBase);
        assertThat(TestUtils.getAllRoles(mongoConnection)).isEmpty();
    }

    @Test
    void testMongoLiquibaseDropAll() throws LiquibaseException {
        final Liquibase liquiBase = new Liquibase("liquibase/ext/changelog.create-users.test.xml", new ClassLoaderResourceAccessor(), database);

        final DropCollectionsCommand dropCollectionsCommand = new DropCollectionsCommand();
        dropCollectionsCommand.setDatabase(database);
        CommandFactory.getInstance().register(dropCollectionsCommand);
        liquiBase.dropAll();
    }
}