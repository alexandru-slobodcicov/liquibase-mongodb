package liquibase.ext.mongodb.changelog;

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
import liquibase.Scope;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import liquibase.database.core.H2Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.AbstractMongoIntegrationTest;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DATABASE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoHistoryServiceIT extends AbstractMongoIntegrationTest {

    private static final String FILE_PATH = "liquibase/ext/changelog.create-collection.test.xml";
    private static Liquibase LIQUIBASE;

    public static MongoHistoryService mongoHistoryService;

    @BeforeEach
    protected void setUp() throws DatabaseException {
        super.setUp();
        mongoHistoryService = (MongoHistoryService) ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database);
        mongoHistoryService.reset();
        mongoHistoryService.resetDeploymentId();
    }

    private static void initLiquibase() throws Exception {
        LIQUIBASE = new Liquibase(FILE_PATH, new ClassLoaderResourceAccessor(), database);
        LIQUIBASE.update("");

        Scope.getCurrentScope().getSingleton(ExecutorService.class).setExecutor(database, mongoExecutor);
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
    void testReplaceChecksum() throws Exception {
        initLiquibase();

        final ChangeSet changeSet = LIQUIBASE.getDatabaseChangeLog().getChangeSet(FILE_PATH, "alex", "1");
        assertTrue(mongoHistoryService.isServiceInitialized());
        Scope.getCurrentScope().getSingleton(ExecutorService.class).setExecutor(database, mongoExecutor);

        mongoHistoryService.replaceChecksum(changeSet);

        assertFalse(mongoHistoryService.isServiceInitialized());
    }

    @Test
    void testGetRanChangeSet() throws DatabaseException {
        mongoHistoryService.getRanChangeSets();
    }

    @Test
    void testSetExecType() {
    }

    @Test
    void testRemoveFromHistory() throws Exception {
        initLiquibase();

        final ChangeSet changeSet = LIQUIBASE.getDatabaseChangeLog().getChangeSet(FILE_PATH, "alex", "1");

        assertThat(mongoHistoryService.getRanChangeSets().size(), is(1));

        mongoHistoryService.removeFromHistory(changeSet);

        assertTrue(mongoHistoryService.getRanChangeSets().isEmpty());
        assertTrue(mongoHistoryService.isServiceInitialized());
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
