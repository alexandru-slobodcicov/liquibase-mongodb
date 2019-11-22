package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = true)
public class DropCollectionStatement extends AbstractMongoStatement {

    public static final String COMMAND = "drop";

    private final String collectionName;

    @Override
    public String toJs() {
        return
                new StringBuilder()
                        .append("db.")
                        .append(collectionName)
                        .append(".")
                        .append(COMMAND)
                        .append("(")
                        .append(");")
                        .toString();
    }

    @Override
    public void execute(MongoDatabase db) {
        final MongoCollection<Document> collection = db.getCollection(collectionName);
        collection.drop();
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}
