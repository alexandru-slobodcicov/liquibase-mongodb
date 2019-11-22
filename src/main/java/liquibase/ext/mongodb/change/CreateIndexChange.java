package liquibase.ext.mongodb.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.mongodb.statement.CreateIndexStatement;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@DatabaseChange(name = "createIndex",
        description = "Creates index for collection" +
                "https://docs.mongodb.com/manual/reference/method/db.collection.createIndex/#db.collection.createIndex",
        priority = ChangeMetaData.PRIORITY_DATABASE, appliesTo = "collection")
@NoArgsConstructor
@Getter
@Setter
public class CreateIndexChange extends AbstractMongoChange {

    private String collectionName;
    private String keys;
    private String options;

    @Override
    public String getConfirmationMessage() {
        return null;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{
                new CreateIndexStatement(collectionName, keys, options)
        };
    }

}