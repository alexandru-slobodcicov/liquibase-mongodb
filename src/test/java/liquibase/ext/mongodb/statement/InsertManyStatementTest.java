package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import liquibase.exception.DatabaseException;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class InsertManyStatementTest extends AbstractMongoIntegrationTest {

    @Test
    void toStringTest() {
        final InsertManyStatement statement = new InsertManyStatement(COLLECTION_NAME_1, Collections.emptyList(), new Document());
        assertThat(statement.toJs())
            .isEqualTo(statement.toString())
            .isEqualTo("db.collectionName.insertMany([], {});");
    }

    @Test
    void executeForList() throws DatabaseException {
        final MongoDatabase database = mongoConnection.getDb();
        final List<Document> testObjects = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Collections.singletonMap("id", (Object) id))
            .map(Document::new)
            .collect(Collectors.toList());
        new InsertManyStatement(COLLECTION_NAME_1, testObjects, new Document()).execute(database);

        assertThat(database.getCollection(COLLECTION_NAME_1).find())
            .hasSize(5);
    }

    @Test
    void executeForString() throws DatabaseException {
        final MongoDatabase database = mongoConnection.getDb();
        final String testObjects = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Collections.singletonMap("id", (Object) id))
            .map(Document::new)
            .map(Document::toJson)
            .collect(Collectors.joining(",", "[", "]"));
        new InsertManyStatement(COLLECTION_NAME_1, testObjects, "").execute(database);

        assertThat(database.getCollection(COLLECTION_NAME_1).find())
            .hasSize(5);
    }
}