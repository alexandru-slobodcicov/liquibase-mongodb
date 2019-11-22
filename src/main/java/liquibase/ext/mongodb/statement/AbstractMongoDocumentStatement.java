package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public abstract class AbstractMongoDocumentStatement<T extends Document> extends AbstractMongoStatement {

    public abstract T run(final MongoDatabase db);

    @Override
    public void execute(MongoDatabase db) {
        run(db);
    }

}
