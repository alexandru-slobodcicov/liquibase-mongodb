package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.stream.StreamSupport;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = true)
public class CountCollectionByNameStatement extends AbstractMongoStatement {

    public static final String COMMAND = "listCollectionNames";

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
        return StreamSupport.stream(db.listCollectionNames().spliterator(), false)
                .filter(s -> s.equals(collectionName))
                .count();
    }

    @Override
    public String toString() {
        return toJs();
    }
}
