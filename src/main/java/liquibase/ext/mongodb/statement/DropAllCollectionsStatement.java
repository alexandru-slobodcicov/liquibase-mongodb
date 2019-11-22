package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import java.util.function.Consumer;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = true)
public class DropAllCollectionsStatement extends AbstractMongoStatement {

    public static final String COMMAND = "dropAll";

    @Override
    public String toJs() {
        return
                new StringBuilder()
                        .append("db.")
                        .append(COMMAND)
                        .append("(")
                        .append(");")
                        .toString();
    }

    @Override
    public void execute(MongoDatabase db) {
        db.listCollectionNames()
            .map(db::getCollection)
            .forEach((Consumer<? super MongoCollection<Document>>) MongoCollection::drop);
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}