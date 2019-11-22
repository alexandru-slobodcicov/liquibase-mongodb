package liquibase.ext.mongodb.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.mongodb.statement.AdminCommandStatement;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@DatabaseChange(name = "adminCommand",
    description = "Provides a helper to run specified database commands against the admin database. " +
        "https://docs.mongodb.com/manual/reference/method/db.adminCommand/#db.adminCommand",
    priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "admin")
@NoArgsConstructor
@Getter
@Setter
public class AdminCommandChange extends AbstractMongoChange {

    private String command;

    @Override
    public String getConfirmationMessage() {
        return null;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{
            new AdminCommandStatement(command)
        };
    }
}