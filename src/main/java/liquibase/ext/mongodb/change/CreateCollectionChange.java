package liquibase.ext.mongodb.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.mongodb.statement.CreateCollectionStatement;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@DatabaseChange(name = "createCollection",
    description = "Create collection with validation " +
        "https://docs.mongodb.com/manual/reference/method/db.createCollection/#db.createCollection",
    priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "collection")
@NoArgsConstructor
@Getter
@Setter
public class CreateCollectionChange extends AbstractMongoChange {

    private String collectionName;
    private String options;

    @Override
    public String getConfirmationMessage() {
        return null;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{
            new CreateCollectionStatement(collectionName, options)
        };
    }
}