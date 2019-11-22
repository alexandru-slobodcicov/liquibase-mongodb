package liquibase.ext.mongodb.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.mongodb.statement.InsertManyStatement;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@DatabaseChange(name = "insertMany",
        description = "Inserts multiple documents into a collection " +
                "https://docs.mongodb.com/manual/reference/method/db.collection.insertMany/#db.collection.insertMany",
        priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "collection")
@NoArgsConstructor
@Getter
@Setter
public class InsertManyChange extends AbstractMongoChange {

    private String collectionName;
    private String documents;
    private String options;

    @Override
    public String getConfirmationMessage() {
        return null;
    }

    @Override
    public SqlStatement[] generateStatements(final Database database) {

        return new SqlStatement[]{
                new InsertManyStatement(collectionName, documents, options)
        };
    }
}
