package liquibase.ext;

import liquibase.Liquibase;
import liquibase.command.CommandFactory;
import liquibase.exception.LiquibaseException;
import liquibase.ext.mongodb.database.DropCollectionsCommand;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class MongoLiquibaseIntegrationTest extends AbstractMongoIntegrationTest {

    @Test
    @Disabled
        // FixMe
    void testMongoLiquibase() throws LiquibaseException {
        Liquibase liquiBase = new Liquibase("liquibase/ext/changelog.insert-one.test.xml", new ClassLoaderResourceAccessor(), database);
        liquiBase.update("");
    }

    @Test
    public void testMongoLiquibaseDropAll() throws LiquibaseException {
        Liquibase liquiBase = new Liquibase("liquibase/ext/changelog.insert-one.test.xml", new ClassLoaderResourceAccessor(), database);
        final DropCollectionsCommand dropCollectionsCommand = new DropCollectionsCommand();
        dropCollectionsCommand.setDatabase(database);
        CommandFactory.getInstance().register(dropCollectionsCommand);
        liquiBase.dropAll();
    }

}
