package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = true)
public class CountDocumentsInCollection extends AbstractMongoStatement {

    public static final String COMMAND = "countDocumentsInCollection";

    private final String collectionName;

    @Override
    public String toJs() {
        return
                new StringBuilder()
                        .append("db.")
                        .append(COMMAND)
                        .append("(")
                        .append(collectionName)
                        .append(");")
                        .toString();
    }

    @Override
    public long queryForLong(MongoDatabase db) {
        return db.getCollection(collectionName).countDocuments();
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}
