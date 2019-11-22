package liquibase.ext.mongodb.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.mongodb.statement.RunCommandStatement;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@DatabaseChange(name = "runCommand",
    description = "Provides a helper to run specified database commands. This is the preferred method to issue database commands, as it provides a consistent interface between the shell and drivers. " +
        "https://docs.mongodb.com/manual/reference/method/db.runCommand/#db-runcommand",
    priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "database")
@NoArgsConstructor
@Getter
@Setter
public class RunCommandChange extends AbstractMongoChange {

    private String command;

    @Override
    public String getConfirmationMessage() {
        return null;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{
            new RunCommandStatement(command)
        };
    }
}