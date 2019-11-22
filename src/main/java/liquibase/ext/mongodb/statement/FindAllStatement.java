package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import liquibase.exception.DatabaseException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;

@Getter
@EqualsAndHashCode(callSuper = true)
public class FindAllStatement extends AbstractMongoStatement {

    public static final String COMMAND = "find";

    private final String collectionName;
    private final Document filter;
    private final Document sort;

    public FindAllStatement(final String collectionName) {
        this(collectionName, new Document(), new Document());
    }

    public FindAllStatement(final String collectionName, final String filter, final String sort) {
        this(collectionName, orEmptyDocument(filter), orEmptyDocument(sort));
    }

    public FindAllStatement(final String collectionName, final Document filter, final Document sort) {
        this.collectionName = collectionName;
        this.filter = filter;
        this.sort = sort;
    }

    @Override
    public String toJs() {
        return
            "db." +
                collectionName +
                "." +
                COMMAND +
                "(" +
                filter.toJson() +
                ");";
    }

    @Override
    public List queryForList(final MongoDatabase db, final Class elementType) throws DatabaseException {
        final ArrayList result = new ArrayList();
        db.getCollection(collectionName, elementType)
            .find(filter).sort(sort).into(result);
        return result;
    }

    @Override
    public String toString() {
        return toJs();
    }
}
