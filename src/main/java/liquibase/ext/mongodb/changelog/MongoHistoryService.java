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

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import liquibase.Scope;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.ext.mongodb.lockservice.CreateChangeLogLockCollectionStatement;
import liquibase.ext.mongodb.statement.*;
import liquibase.logging.Logger;
import liquibase.nosql.changelog.AbstractNoSqlHistoryService;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.stream.Collectors;

public class MongoHistoryService extends AbstractNoSqlHistoryService {

    private final Logger log = Scope.getCurrentScope().getLog(getClass());

    @Getter
    private final MongoRanChangeSetToDocumentConverter converter;

    public MongoHistoryService() {
        super();
        this.converter = new MongoRanChangeSetToDocumentConverter();
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public boolean supports(final Database database) {
        return MongoLiquibaseDatabase.MONGODB_PRODUCT_NAME.equals(database.getDatabaseProductName());
    }

    @Override
    protected Boolean existsRepository() throws DatabaseException {
        return getExecutor().queryForLong(
                new CountCollectionByNameStatement(getDatabaseChangeLogTableName())) == 1L;
    }

    @Override
    protected void createRepository() throws DatabaseException {
        final CreateChangeLogLockCollectionStatement createChangeLogLockCollectionStatement =
                new CreateChangeLogLockCollectionStatement(getDatabaseChangeLogTableName());
        getExecutor().execute(createChangeLogLockCollectionStatement);
    }

    @Override
    protected void adjustRepository() throws DatabaseException {
        if (((MongoLiquibaseDatabase)getDatabase()).getAdjustTrackingTablesOnStartup()) {
            this.getLogger().info("Adjusting database history Collection with name: "
                    + getDatabase().getConnection().getCatalog() + "." + getDatabaseChangeLogTableName());

            this.getLogger().info("Adjusted database history Collection with name: "
                    + getDatabase().getConnection().getCatalog() + "." + getDatabaseChangeLogTableName());

            getExecutor().execute(new AdjustChangeLogCollectionStatement(getDatabaseChangeLogTableName(),
                    ((MongoLiquibaseDatabase)getDatabase()).getSupportsValidator()));

        } else {
            this.getLogger().info("Skipped Adjusting database history Collection with name: "
                    + getDatabase().getConnection().getCatalog() + "." + getDatabaseChangeLogTableName());
        }
    }

    @Override
    protected void dropRepository() throws DatabaseException {
        getExecutor().execute(
                new DropCollectionStatement(getDatabaseChangeLogTableName()));
    }

    @Override
    protected List<RanChangeSet> queryRanChangeSets() throws DatabaseException {

        return getExecutor()
                .queryForList(new FindAllStatement(getDatabaseChangeLogTableName()), Document.class)
                .stream().map(Document.class::cast).map(converter::fromDocument).collect(Collectors.toList());
    }

    @Override
    protected Integer generateNextSequence() throws DatabaseException {
        return (int) countRanChangeSets();
    }

    @Override
    protected void markChangeSetRun(ChangeSet changeSet, ChangeSet.ExecType execType, Integer nextSequenceValue) throws DatabaseException {
        if (execType.ranBefore) {
            final Document filter = new Document()
                    .append(MongoRanChangeSet.Fields.fileName, changeSet.getFilePath())
                    .append(MongoRanChangeSet.Fields.changeSetId, changeSet.getId())
                    .append(MongoRanChangeSet.Fields.author, changeSet.getAuthor());

            final Document update = new Document()
                    .append(MongoRanChangeSet.Fields.execType, execType.value);

            getExecutor().execute(new UpdateManyStatement(getDatabaseChangeLogTableName(), update, filter));

        } else {
            final MongoRanChangeSet ranChangeSet = new MongoRanChangeSet(changeSet, execType, null, null);
            ranChangeSet.setOrderExecuted(getNextSequenceValue());
            getExecutor().execute(new InsertOneStatement(getDatabaseChangeLogTableName(),
                    converter.toDocument(ranChangeSet), new Document()));
        }
    }

    @Override
    protected void removeRanChangeSet(ChangeSet changeSet) throws DatabaseException {
        final Document filter = new Document()
                .append(MongoRanChangeSet.Fields.fileName, changeSet.getFilePath())
                .append(MongoRanChangeSet.Fields.changeSetId, changeSet.getId())
                .append(MongoRanChangeSet.Fields.author, changeSet.getAuthor());

        getExecutor().execute(new DeleteManyStatement(getDatabaseChangeLogTableName(), filter));
    }

    @Override
    protected void clearChekSums() throws DatabaseException {
        final Document filter = new Document();
        final Document update = new Document().append(MongoRanChangeSet.Fields.md5sum, StringUtils.EMPTY);

        getExecutor().update(new UpdateManyStatement(getDatabaseChangeLogTableName(), update, filter));
    }

    @Override
    protected long countTags(final String tag) throws DatabaseException {
        final Bson filter = Filters.eq(MongoRanChangeSet.Fields.tag, tag);
        return getExecutor().queryForLong(
                new CountDocumentsInCollectionStatement(getDatabaseChangeLogTableName(), filter));
    }

    @Override
    protected long countRanChangeSets() throws DatabaseException {
        return getExecutor().queryForLong(new CountDocumentsInCollectionStatement(getDatabaseChangeLogTableName()));
    }

    @Override
    protected void updateCheckSum(final ChangeSet changeSet) throws DatabaseException {
        final Bson filter = Filters.and(
                Filters.eq(MongoRanChangeSet.Fields.fileName, changeSet.getFilePath()),
                Filters.eq(MongoRanChangeSet.Fields.changeSetId, changeSet.getId()),
                Filters.eq(MongoRanChangeSet.Fields.author, changeSet.getAuthor())
        );

        final Bson update = Updates.set(MongoRanChangeSet.Fields.md5sum, changeSet.generateCheckSum().toString());

        getExecutor().update(new UpdateManyStatement(getDatabaseChangeLogTableName(), filter, update));
    }
}
