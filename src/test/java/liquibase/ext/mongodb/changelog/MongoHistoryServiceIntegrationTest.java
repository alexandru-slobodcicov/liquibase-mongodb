package liquibase.ext.mongodb.changelog;

import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.RanChangeSet;
import liquibase.database.core.H2Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DATABASE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class MongoHistoryServiceIntegrationTest extends AbstractMongoIntegrationTest {

    public static MongoHistoryService mongoHistoryService;

    @BeforeEach
    protected void setUp() throws DatabaseException {
        super.setUp();
        mongoHistoryService = (MongoHistoryService) ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database);
        mongoHistoryService.reset();
        mongoHistoryService.resetDeploymentId();
    }

    @Test
    void testGetPriority() {
        assertThat(mongoHistoryService.getPriority(), equalTo(PRIORITY_DATABASE));
    }

    @Test
    void testSupports() {
        assertThat(mongoHistoryService.supports(database), equalTo(true));
        assertThat(mongoHistoryService.supports(new H2Database()), equalTo(false));
    }

    @Test
    void testGetDatabaseChangeLogTableName() {
        assertThat(mongoHistoryService.getDatabaseChangeLogTableName(), equalTo(database.getDatabaseChangeLogTableName()));
    }

    @Test
    void testCanCreateChangeLogTable() {
        assertThat(mongoHistoryService.canCreateChangeLogTable(), equalTo(true));
    }

    @Test
    void testInit() throws DatabaseException {
        assertThat(mongoHistoryService.getRanChangeSetList(), nullValue());
        assertThat(mongoHistoryService.isServiceInitialized(), equalTo(false));
        assertThat(mongoHistoryService.getHasDatabaseChangeLogTable(), nullValue());
        mongoHistoryService.init();
        assertThat(mongoHistoryService.getRanChangeSetList(), nullValue());
        assertThat(mongoHistoryService.isServiceInitialized(), equalTo(true));
        assertThat(mongoHistoryService.getHasDatabaseChangeLogTable(), equalTo(false));
    }

    @Test
    void testUpgradeChecksums() {
    }

    @Test
    void testGetRanChangeSets() throws DatabaseException {
        mongoHistoryService.init();
        assertThat(mongoHistoryService.getRanChangeSetList(), nullValue());

        final List<RanChangeSet> ranChangeSetList = mongoHistoryService.getRanChangeSetList();
        assertThat(ranChangeSetList, nullValue());
    }

    @Test
    void testQueryDatabaseChangeLogTable() throws DatabaseException {

    }

    @Test
    void testReplaceChecksum() {
    }

    @Test
    void testGetRanChangeSet() throws DatabaseException {
        mongoHistoryService.getRanChangeSets();
    }

    @Test
    void testSetExecType() {
    }

    @Test
    void testRemoveFromHistory() {
    }

    @Test
    void testGetNextSequenceValue() {
    }

    @Test
    void testTag() {
    }

    @Test
    void testTagExists() {
    }

    @Test
    void testClearAllCheckSums() {
    }

    @Test
    void testDestroy() throws DatabaseException {
        assertThat(mongoHistoryService.getRanChangeSetList(), nullValue());
        assertThat(mongoHistoryService.isServiceInitialized(), equalTo(false));
        assertThat(mongoHistoryService.getHasDatabaseChangeLogTable(), nullValue());
        mongoHistoryService.init();
        assertThat(mongoHistoryService.getRanChangeSetList(), nullValue());
        assertThat(mongoHistoryService.isServiceInitialized(), equalTo(true));
        assertThat(mongoHistoryService.getHasDatabaseChangeLogTable(), equalTo(false));
        mongoHistoryService.destroy();
        assertThat(mongoHistoryService.getRanChangeSetList(), nullValue());
        assertThat(mongoHistoryService.isServiceInitialized(), equalTo(false));
        assertThat(mongoHistoryService.getHasDatabaseChangeLogTable(), nullValue());
        assertThat(mongoHistoryService.hasDatabaseChangeLogTable(), equalTo(false));
    }
}