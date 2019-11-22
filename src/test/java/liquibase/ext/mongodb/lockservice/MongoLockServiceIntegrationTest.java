package liquibase.ext.mongodb.lockservice;

import liquibase.exception.DatabaseException;
import liquibase.exception.LockException;
import liquibase.ext.AbstractMongoIntegrationTest;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.lockservice.LockServiceFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

class MongoLockServiceIntegrationTest extends AbstractMongoIntegrationTest {

    public MongoLockService mongoLockService;

    @BeforeEach
    protected void setUp() throws DatabaseException {
        super.setUp();
        mongoLockService = (MongoLockService) LockServiceFactory.getInstance().getLockService(database);
        mongoLockService.reset();
    }

    @Test
    void testGetDatabaseTest() {
        assertThat(mongoLockService, instanceOf(MongoLockService.class));
        assertThat(mongoLockService.getDatabase(), instanceOf(MongoLiquibaseDatabase.class));
        assertThat(mongoLockService.getDatabase(), equalTo(database));
    }

    @Test
    void testInit() throws DatabaseException, LockException {
        mongoLockService.reset();
        assertThat(mongoLockService.hasChangeLogLock(), equalTo(false));
        assertThat(mongoLockService.getHasDatabaseChangeLogLockTable(), nullValue());
        mongoLockService.init();
        assertThat(mongoLockService.hasChangeLogLock(), equalTo(false));
        assertThat(mongoLockService.getHasDatabaseChangeLogLockTable(), equalTo(true));
    }

    @Test
    void testAcquireLock() throws DatabaseException, LockException {
        mongoLockService.reset();
        assertThat(mongoLockService.hasChangeLogLock(), equalTo(false));
        mongoLockService.init();
        assertThat(mongoLockService.hasChangeLogLock(), equalTo(false));
        final boolean acquiredLock = mongoLockService.acquireLock();
        assertThat(acquiredLock, equalTo(true));
        assertThat(mongoLockService.hasChangeLogLock(), equalTo(true));
        final DatabaseChangeLogLock[] locks = mongoLockService.listLocks();
        assertThat(locks.length, equalTo(1));
    }

    @Test
    void waitForLock() throws LockException {
        mongoLockService.waitForLock();
    }

    @Test
    void releaseLock() {
        //TODO:
    }

    @Test
    void forceReleaseLock() {
        //TODO:
    }
}