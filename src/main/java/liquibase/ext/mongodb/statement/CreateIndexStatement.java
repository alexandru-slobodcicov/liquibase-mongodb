package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;

@Getter
@EqualsAndHashCode(callSuper = true)
public class CreateIndexStatement extends AbstractMongoStatement {

    public static final String COMMAND = "createIndex";

    private final String collectionName;
    private final Document keys;
    private final Document options;

    public CreateIndexStatement(final String collectionName, final Document keys, final Document options) {
        this.collectionName = collectionName;
        this.keys = keys;
        this.options = options;
    }

    public CreateIndexStatement(final String collectionName, final String keys, final String options) {
        this(collectionName, orEmptyDocument(keys), orEmptyDocument(options));
    }

    @Override
    public String toJs() {
        return
                "db."
                        + collectionName
                        + ". "
                        + COMMAND
                        + "("
                        + keys.toJson()
                        + ", "
                        + options.toJson()
                        + ");";
    }

    @Override
    public void execute(MongoDatabase db) {
        db.getCollection(collectionName).createIndex(keys, createIndexOptions(options));
    }

    private IndexOptions createIndexOptions(final Document indexOptions) {
        //TODO: add POJO codec
        final IndexOptions options = new IndexOptions();
        if (indexOptions.containsKey("unique") && indexOptions.getBoolean("unique")) {
            options.unique(true);
        }
        if (indexOptions.containsKey("name")) {
            options.name(indexOptions.getString("name"));
        }
        return options;
    }

    @Override
    public String toString() {
        return toJs();
    }
}
