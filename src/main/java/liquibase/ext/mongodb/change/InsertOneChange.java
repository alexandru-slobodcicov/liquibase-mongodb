package liquibase.ext.mongodb.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.mongodb.statement.InsertOneStatement;
import liquibase.statement.SqlStatement;
import liquibase.util.StringUtils;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bson.Document;

@DatabaseChange(name = "insertOne",
        description = "Insert a Single Document " +
                "https://docs.mongodb.com/manual/tutorial/insert-documents/#insert-a-single-document",
        priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "collection")
@NoArgsConstructor
@Getter
@Setter
public class InsertOneChange extends AbstractMongoChange {

    private String collectionName;
    private String document;
    private String options;

    @Override
    public String getConfirmationMessage() {
        return null;
    }

    @Override
    public SqlStatement[] generateStatements(final Database database) {
        return new SqlStatement[]{
                new InsertOneStatement(collectionName, document, options)
        };
    }
}
