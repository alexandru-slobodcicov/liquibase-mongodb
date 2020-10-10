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
import liquibase.database.core.H2Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.AbstractMongoIntegrationTest;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoHistoryServiceIT extends AbstractMongoIntegrationTest {

    private static final String FILE_PATH = "liquibase/ext/changelog.create-collection.test.xml";
    private Liquibase liquibase;

    public MongoHistoryService mongoHistoryService;

    @BeforeEach
    protected void setUpEach() {
        super.setUpEach();
        mongoHistoryService = (MongoHistoryService) ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database);
        mongoHistoryService.reset();
        mongoHistoryService.resetDeploymentId();
    }

    private void initLiquibase() throws Exception {
        liquibase = new Liquibase(FILE_PATH, new ClassLoaderResourceAccessor(), database);
        liquibase.update("");

        Scope.getCurrentScope().getSingleton(ExecutorService.class).setExecutor(database, executor);
    }

    @Test
    void testGetPriority() {
        assertThat(mongoHistoryService.getPriority()).isEqualTo(PRIORITY_DATABASE);
    }

    @Test
    void testSupports() {
        assertThat(mongoHistoryService.supports(database)).isTrue();
        assertThat(mongoHistoryService.supports(new H2Database())).isFalse();
    }

    @Test
    void testGetDatabaseChangeLogTableName() {
        assertThat(mongoHistoryService.getDatabaseChangeLogTableName()).isEqualTo("DATABASECHANGELOG");
        assertThat(mongoHistoryService.getDatabaseChangeLogTableName()).isEqualTo(database.getDatabaseChangeLogTableName());
    }

    @Test
    void testCanCreateChangeLogTable() {
        assertThat(mongoHistoryService.canCreateChangeLogTable()).isTrue();
    }

    @Test
    void testInit() throws DatabaseException {
        assertThat(mongoHistoryService.existsRepository()).isFalse();
        assertThat(mongoHistoryService.countRanChangeSets()).isEqualTo(0L);
        assertThat(mongoHistoryService.isServiceInitialized()).isFalse();

        mongoHistoryService.init();
        assertThat(mongoHistoryService.existsRepository()).isTrue();
        assertThat(mongoHistoryService.countRanChangeSets()).isEqualTo(0L);
        assertThat(mongoHistoryService.isServiceInitialized()).isTrue();
    }

    @Test
    void testUpgradeChecksums() {
    }

    @Test
    void testGetRanChangeSets() throws DatabaseException {
        mongoHistoryService.init();
        assertThat(mongoHistoryService.existsRepository()).isTrue();
        assertThat(mongoHistoryService.countRanChangeSets()).isEqualTo(0L);
        assertThat(mongoHistoryService.isServiceInitialized()).isTrue();
    }

    @Test
    void testQueryDatabaseChangeLogTable() throws DatabaseException {

    }

    @Test
    void testReplaceChecksum() throws Exception {
        initLiquibase();

        final ChangeSet changeSet = liquibase.getDatabaseChangeLog().getChangeSet(FILE_PATH, "alex", "1");
        assertTrue(mongoHistoryService.isServiceInitialized());
        Scope.getCurrentScope().getSingleton(ExecutorService.class).setExecutor(database, executor);

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

        final ChangeSet changeSet = liquibase.getDatabaseChangeLog().getChangeSet(FILE_PATH, "alex", "1");

        assertThat(mongoHistoryService.getRanChangeSets()).hasSize(1);

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
        assertThat(mongoHistoryService.existsRepository()).isFalse();
        assertThat(mongoHistoryService.countRanChangeSets()).isEqualTo(0L);
        assertThat(mongoHistoryService.isServiceInitialized()).isFalse();

        mongoHistoryService.init();
        assertThat(mongoHistoryService.existsRepository()).isTrue();
        assertThat(mongoHistoryService.countRanChangeSets()).isEqualTo(0L);
        assertThat(mongoHistoryService.isServiceInitialized()).isTrue();

        mongoHistoryService.destroy();
        assertThat(mongoHistoryService.existsRepository()).isFalse();
        assertThat(mongoHistoryService.countRanChangeSets()).isEqualTo(0L);
        assertThat(mongoHistoryService.isServiceInitialized()).isFalse();
        assertThat(mongoHistoryService.hasDatabaseChangeLogTable()).isFalse();
    }
}
