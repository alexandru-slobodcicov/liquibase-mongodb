package liquibase.ext.mongodb.statement;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import liquibase.exception.DatabaseException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;
import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyList;

@Getter
@EqualsAndHashCode(callSuper = true)
public class InsertManyStatement extends AbstractMongoStatement {

    public static final String COMMAND = "insertMany";

    private final String collectionName;
    private final List<Document> documents;
    private Document options;

    public InsertManyStatement(final String collectionName, final String documents, final String options) {
        this(collectionName, new ArrayList<>(orEmptyList(documents)), orEmptyDocument(options));
    }

    public InsertManyStatement(final String collectionName, final List<Document> documents, final Document options) {
        this.collectionName = collectionName;
        this.documents = documents;
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
                        documents.toString() +
                        ", " +
                        options.toJson() +
                        ");";
    }

    @Override
    public void execute(MongoDatabase db) throws DatabaseException {
        try {
            final MongoCollection<Document> collection = db.getCollection(collectionName);
            collection.insertMany(documents);
            //TODO: Parse options into POJO InsertManyOptions.class
        } catch (MongoException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}
