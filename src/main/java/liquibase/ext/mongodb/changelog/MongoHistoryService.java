package liquibase.ext.mongodb.changelog;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.changelog.AbstractChangeLogHistoryService;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.DatabaseHistoryException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.ext.mongodb.statement.AbstractMongoStatement;
import liquibase.ext.mongodb.statement.CountCollectionByNameStatement;
import liquibase.ext.mongodb.statement.DropCollectionStatement;
import liquibase.logging.LogService;
import liquibase.logging.LogType;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class MongoHistoryService extends AbstractChangeLogHistoryService {

    protected static final String LABELS_SIZE = "255";
    protected static final String CONTEXTS_SIZE = "255";
    private List<RanChangeSet> ranChangeSetList;
    private boolean serviceInitialized;
    private Boolean hasDatabaseChangeLogTable;
    private boolean databaseChecksumsCompatible;
    private Integer lastChangeSetSequenceValue;

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return MongoLiquibaseDatabase.MONGODB_PRODUCT_NAME.equals(database.getDatabaseProductName());
    }

    public String getDatabaseChangeLogTableName() {
        return getDatabase().getDatabaseChangeLogTableName();
    }

    public String getLiquibaseSchemaName() {
        return getDatabase().getLiquibaseSchemaName();
    }

    public String getLiquibaseCatalogName() {
        return getDatabase().getLiquibaseCatalogName();
    }

    public boolean canCreateChangeLogTable() {
        return true;
    }

    public boolean isDatabaseChecksumsCompatible() {
        return databaseChecksumsCompatible;
    }

    public Boolean getHasDatabaseChangeLogTable() {
        return hasDatabaseChangeLogTable;
    }

    public List<RanChangeSet> getRanChangeSetList() {
        return ranChangeSetList;
    }

    public boolean isServiceInitialized() {
        return serviceInitialized;
    }

    @Override
    public void reset() {
        this.ranChangeSetList = null;
        this.serviceInitialized = false;
        this.hasDatabaseChangeLogTable = null;
    }

    public boolean hasDatabaseChangeLogTable() {
        if (hasDatabaseChangeLogTable == null) {
            try {
                final Executor executor = ExecutorService.getInstance().getExecutor(getDatabase());
                hasDatabaseChangeLogTable =
                        executor.queryForLong(new CountCollectionByNameStatement(getDatabase().getDatabaseChangeLogTableName())) == 1L;
            } catch (LiquibaseException e) {
                throw new UnexpectedLiquibaseException(e);
            }
        }
        return hasDatabaseChangeLogTable;
    }

    public void init() throws DatabaseException {
        if (serviceInitialized) {
            return;
        }

        final Executor executor = ExecutorService.getInstance().getExecutor(getDatabase());

        boolean createdTable = hasDatabaseChangeLogTable();

        if (createdTable) {
            //TODO: Add MD5SUM check logic and potentially get and check validator structure and update not equal
        } else {
            executor.comment("Create Database Change Log Collection");

            AbstractMongoStatement createChangeLogCollectionStatement =
                    new CreateChangeLogCollectionStatement(getDatabase().getDatabaseChangeLogTableName());

            // If there is no table in the database for recording change history create one.
            LogService.getLog(getClass()).info(LogType.LOG, "Creating database history collection with name: " +
                    getDatabase().getLiquibaseCatalogName() + "." + getDatabase().getDatabaseChangeLogTableName());

            executor.execute(createChangeLogCollectionStatement);

            LogService.getLog(getClass()).info(LogType.LOG, "Created database history collection : " +
                    createChangeLogCollectionStatement.toJs());

            getDatabase().commit();
        }

        this.serviceInitialized = true;
    }

    @Override
    public void upgradeChecksums(final DatabaseChangeLog databaseChangeLog, final Contexts contexts, LabelExpression
            labels) throws DatabaseException {
        super.upgradeChecksums(databaseChangeLog, contexts, labels);
        getDatabase().commit();
    }

    /**
     * Returns the ChangeSets that have been run against the current getDatabase().
     */
    public List<RanChangeSet> getRanChangeSets() throws DatabaseException {

        if (this.ranChangeSetList == null) {
//            Database database = getDatabase();
//            String databaseChangeLogTableName = getDatabase().escapeTableName(getLiquibaseCatalogName(),
//                    getLiquibaseSchemaName(), getDatabaseChangeLogTableName());
//            List<RanChangeSet> ranChangeSets = new ArrayList<>();
//            if (hasDatabaseChangeLogTable()) {
//                LogService.getLog(getClass()).info(LogType.LOG, "Reading from " + databaseChangeLogTableName);
//                List<Map<String, ?>> results = queryDatabaseChangeLogTable(database);
//                for (Map rs : results) {
//                    String fileName = rs.get("FILENAME").toString();
//                    String author = rs.get("AUTHOR").toString();
//                    String id = rs.get("ID").toString();
//                    String md5sum = ((rs.get("MD5SUM") == null) || !databaseChecksumsCompatible) ? null : rs.get
//                            ("MD5SUM").toString();
//                    String description = (rs.get("DESCRIPTION") == null) ? null : rs.get("DESCRIPTION").toString();
//                    String comments = (rs.get("COMMENTS") == null) ? null : rs.get("COMMENTS").toString();
//                    Object tmpDateExecuted = rs.get("DATEEXECUTED");
//                    Date dateExecuted = null;
//                    if (tmpDateExecuted instanceof Date) {
//                        dateExecuted = (Date) tmpDateExecuted;
//                    } else {
//                        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                        try {
//                            dateExecuted = df.parse((String) tmpDateExecuted);
//                        } catch (ParseException e) {
//                            // Ignore ParseException and assume dateExecuted == null instead of aborting.
//                        }
//                    }
//                    String tmpOrderExecuted = rs.get("ORDEREXECUTED").toString();
//                    Integer orderExecuted = ((tmpOrderExecuted == null) ? null : Integer.valueOf(tmpOrderExecuted));
//                    String tag = (rs.get("TAG") == null) ? null : rs.get("TAG").toString();
//                    String execType = (rs.get("EXECTYPE") == null) ? null : rs.get("EXECTYPE").toString();
//                    ContextExpression contexts = new ContextExpression((String) rs.get("CONTEXTS"));
//                    Labels labels = new Labels((String) rs.get("LABELS"));
//                    String deploymentId = (String) rs.get("DEPLOYMENT_ID");
//
//                    try {
//                        RanChangeSet ranChangeSet = new RanChangeSet(fileName, id, author, CheckSum.parse(md5sum),
//                                dateExecuted, tag, ChangeSet.ExecType.valueOf(execType), description, comments, contexts,
//                                labels, deploymentId);
//                        ranChangeSet.setOrderExecuted(orderExecuted);
//                        ranChangeSets.add(ranChangeSet);
//                    } catch (IllegalArgumentException e) {
//                        LogService.getLog(getClass()).severe(LogType.LOG, "Unknown EXECTYPE from database: " +
//                                execType);
//                        throw e;
//                    }
//                }
//            }

            final Document sort = new Document().append("dateExecuted", 1).append("orderExecuted", 1);

            Collection<Document> ranChangeSets = new ArrayList<>();
            ((MongoLiquibaseDatabase) getDatabase()).getConnection().getDb().getCollection(getDatabaseChangeLogTableName())
                    .find().sort(sort).into(ranChangeSets);

            this.ranChangeSetList = ranChangeSets.stream().map(ChangeSetUtils::fromDocument).collect(Collectors.toList());
        }
        return Collections.unmodifiableList(ranChangeSetList);
    }

//    public List<Map<String, ?>> queryDatabaseChangeLogTable(Database database) throws DatabaseException {
//
//        //FindAllStatement findAllStatement = new FindAllStatement()
//
//        SelectFromDatabaseChangeLogStatement select = new SelectFromDatabaseChangeLogStatement(new ColumnConfig()
//                .setName("*").setComputed(true)).setOrderBy("DATEEXECUTED ASC", "ORDEREXECUTED ASC");
//        return ExecutorService.getInstance().getExecutor(database).queryForList(select);
//    }

    @Override
    protected void replaceChecksum(ChangeSet changeSet) throws DatabaseException {
        //ExecutorService.getInstance().getExecutor(getDatabase()).execute(new UpdateChangeSetChecksumStatement
        //        (changeSet));

        Document filter = new Document()
                .append("fileName", changeSet.getFilePath())
                .append("id", changeSet.getId())
                .append("author", changeSet.getAuthor());

        Document update = new Document()
                .append("md5sum", changeSet.generateCheckSum().toString());


        ((MongoLiquibaseDatabase) getDatabase()).getConnection().getDb().getCollection(getDatabaseChangeLogTableName())
                .updateOne(filter, update);

        getDatabase().commit();
        reset();
    }

    @Override
    public RanChangeSet getRanChangeSet(final ChangeSet changeSet) throws DatabaseException, DatabaseHistoryException {
        if (!hasDatabaseChangeLogTable()) {
            return null;
        }

        return super.getRanChangeSet(changeSet);
    }

    @Override
    public void setExecType(ChangeSet changeSet, ChangeSet.ExecType execType) throws DatabaseException {


        //TODO: simillar to commented
        //ExecutorService.getInstance().getExecutor(database).execute(new MarkChangeSetRanStatement(changeSet, execType));
        final RanChangeSet ranChangeSet = new RanChangeSet(changeSet, execType, null, null);

        if (execType.ranBefore) {
            Document filter = new Document()
                    .append("fileName", changeSet.getFilePath())
                    .append("id", changeSet.getId())
                    .append("author", changeSet.getAuthor());

            Document update = new Document()
                    .append("execType", execType.value);

            ((MongoLiquibaseDatabase) getDatabase()).getConnection().getDb().getCollection(getDatabaseChangeLogTableName())
                    .updateOne(filter, update);

        } else {
            ranChangeSet.setOrderExecuted(getNextSequenceValue());
            ((MongoLiquibaseDatabase) getDatabase()).getConnection().getDb().getCollection(getDatabaseChangeLogTableName())
                    .insertOne(ChangeSetUtils.toDocument(ranChangeSet));
        }

        getDatabase().commit();
        if (this.ranChangeSetList != null) {
            this.ranChangeSetList.add(ranChangeSet);
        }
    }

    @Override
    public void removeFromHistory(final ChangeSet changeSet) throws DatabaseException {

        //ExecutorService.getInstance().getExecutor(database).execute(new RemoveChangeSetRanStatusStatement(changeSet));

        Document filter = new Document()
                .append("fileName", changeSet.getFilePath())
                .append("id", changeSet.getId())
                .append("author", changeSet.getAuthor());

        ((MongoLiquibaseDatabase) getDatabase()).getConnection().getDb().getCollection(getDatabaseChangeLogTableName())
                .deleteOne(filter);

        getDatabase().commit();

        if (this.ranChangeSetList != null) {
            this.ranChangeSetList.remove(new RanChangeSet(changeSet));
        }
    }

    @Override
    public int getNextSequenceValue() {
        if (lastChangeSetSequenceValue == null) {
            if (getDatabase().getConnection() == null) {
                lastChangeSetSequenceValue = 0;
            } else {
                lastChangeSetSequenceValue = Long.valueOf(((MongoLiquibaseDatabase) getDatabase()).getConnection().getDb().getCollection(getDatabaseChangeLogTableName())
                        .countDocuments()).intValue();
            }
        }

        return ++lastChangeSetSequenceValue;
    }

    /**
     * Tags the database changelog with the given string.
     */
    @Override
    public void tag(final String tagString) throws DatabaseException {
        int totalRows =
                Long.valueOf(((MongoLiquibaseDatabase) getDatabase()).getConnection().getDb().getCollection(getDatabaseChangeLogTableName())
                        .countDocuments()).intValue();
        if (totalRows == 0) {
            ChangeSet emptyChangeSet = new ChangeSet(String.valueOf(new Date().getTime()), "liquibase",
                    false, false, "liquibase-internal", null, null,
                    getDatabase().getObjectQuotingStrategy(), null);
            this.setExecType(emptyChangeSet, ChangeSet.ExecType.EXECUTED);
        }

        //TODO: update the last row tag
        //executor.execute(new TagDatabaseStatement(tagString));
        getDatabase().commit();

        if (this.ranChangeSetList != null) {
            ranChangeSetList.get(ranChangeSetList.size() - 1).setTag(tagString);
        }
    }

    @Override
    public boolean tagExists(final String tag) throws DatabaseException {
        int count = Long.valueOf(((MongoLiquibaseDatabase) getDatabase()).getConnection().getDb().getCollection(getDatabaseChangeLogTableName())
                .countDocuments(new Document("tag", tag))).intValue();
        return count > 0;
    }

    @Override
    public void clearAllCheckSums() throws LiquibaseException {
        Document filter = new Document();

        Document update = new Document()
                .append("md5sum", null);

        ((MongoLiquibaseDatabase) getDatabase()).getConnection().getDb().getCollection(getDatabaseChangeLogTableName())
                .updateMany(filter, update);
        getDatabase().commit();
    }

    @Override
    public void destroy() throws DatabaseException {

        try {
            final Executor executor = ExecutorService.getInstance().getExecutor(getDatabase());

            executor.comment("Dropping Collection Database Change Log: " + getDatabaseChangeLogTableName());
            {
                executor.execute(
                        new DropCollectionStatement(getDatabaseChangeLogTableName()));
            }
            getDatabase().commit();
            reset();
        } catch (DatabaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }
}
