package liquibase.ext.mongodb.statement;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import liquibase.exception.DatabaseException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;

@Getter
@EqualsAndHashCode(callSuper = true)
public class InsertOneStatement extends AbstractMongoStatement {

    public static final String COMMAND = "insertOne";

    private final String collectionName;
    private final Document document;
    private final Document options;

    public InsertOneStatement(final String collectionName, final String document, final String options) {
        this(collectionName, orEmptyDocument(document), orEmptyDocument(options));
    }

    public InsertOneStatement(final String collectionName, final Document document, final Document options) {
        this.collectionName = collectionName;
        this.document = document;
        this.options = options;
    }

    @Override
    public String toJs() {
        return
                "db." +
                        collectionName +
                        "." +
                        COMMAND +
                        "(" +
                        document.toJson() +
                        ", " +
                        options.toJson() +
                        ");";
    }

    @Override
    public void execute(MongoDatabase db) throws DatabaseException {
        try {
            final MongoCollection<Document> collection = db.getCollection(collectionName);
            collection.insertOne(document);
            //TODO: Parse options into POJO InsertOneOptions.class
        } catch (MongoException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}
