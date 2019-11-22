package liquibase.ext.mongodb.database;

import liquibase.command.CommandResult;
import liquibase.command.core.DropAllCommand;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.mongodb.statement.DropAllCollectionsStatement;
import liquibase.lockservice.LockService;
import liquibase.lockservice.LockServiceFactory;
import lombok.Setter;

public class DropCollectionsCommand extends DropAllCommand {

    private static final String COMMAND_NAME = "dropAll";

    @Setter
    private Database database;

    @Override
    public String getName() {
        return COMMAND_NAME;
    }

    @Override
    protected CommandResult run() throws Exception {

        LockService lockService = LockServiceFactory.getInstance().getLockService(database);
        try {
            lockService.waitForLock();
            final Executor executor = ExecutorService.getInstance().getExecutor(database);
            executor.execute(new DropAllCollectionsStatement());

        } catch (DatabaseException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseException(e);
        } finally {
            lockService.releaseLock();
            lockService.destroy();
            resetServices();
        }

        return new CommandResult(
            "All objects dropped from " + database.getConnection().getConnectionUserName() + "@" + database.getConnection().getURL());
    }

    @Override
    public int getPriority(String commandName) {
        return COMMAND_NAME.equals(commandName) ? 999 : 0;
    }
}