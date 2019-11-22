package liquibase.ext;

import liquibase.database.DatabaseFactory;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.mongodb.database.MongoConnection;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.ext.mongodb.executor.MongoExecutor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.function.Consumer;

import static liquibase.ext.mongodb.TestUtils.getMongoConnection;

@Slf4j
public class AbstractMongoIntegrationTest {

    protected static final MongoConnection mongoConnection = getMongoConnection("application-test.properties");

    protected static MongoExecutor mongoExecutor;
    protected static MongoLiquibaseDatabase database;

    @BeforeAll
    protected static void init() throws DatabaseException {
    }

    @AfterAll
    protected static void destroy() throws DatabaseException {

        database.getConnection().getDb().listCollectionNames()
            .forEach((Consumer<? super String>) c -> mongoConnection.getDb().getCollection(c).drop());
    }

    @BeforeEach
    protected void setUp() throws DatabaseException {

        //Can be achieved by excluding the package to scan or pass package list via system.parameter
        //ServiceLocator.getInstance().getPackages().remove("liquibase.executor");
        //Another way is to register the executor against a Db

        database = (MongoLiquibaseDatabase) DatabaseFactory.getInstance().findCorrectDatabaseImplementation(mongoConnection);
        database.setConnection(mongoConnection);
        log.debug("database is initialized...");

        mongoExecutor = new MongoExecutor();
        mongoExecutor.setDatabase(database);
        log.debug("mongoExecutor is initialized...");

        ExecutorService.getInstance().setExecutor(database, mongoExecutor);

        database.getConnection().getDb().listCollectionNames()
            .forEach((Consumer<? super String>) c -> mongoConnection.getDb().getCollection(c).drop());
    }
}
