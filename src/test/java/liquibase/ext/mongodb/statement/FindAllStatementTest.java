package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import liquibase.exception.DatabaseException;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class FindAllStatementTest extends AbstractMongoIntegrationTest {

    @ParameterizedTest
    @ValueSource(classes = {Document.class, Map.class})
    void queryForList(final Class clazz) throws DatabaseException {
        final MongoDatabase database = mongoConnection.getDb();
        final List<Document> testObjects = IntStream.rangeClosed(1, 5)
            .mapToObj(id -> Collections.singletonMap("id", (Object) id))
            .map(Document::new)
            .collect(Collectors.toList());
        database.createCollection(COLLECTION_NAME_1);
        database.getCollection(COLLECTION_NAME_1).insertMany(testObjects);

        final FindAllStatement statement = new FindAllStatement(COLLECTION_NAME_1);
        assertThat(statement.queryForList(database, clazz))
            .hasSize(5);
    }

    @Test
    void toStringJs() {
        final FindAllStatement statement = new FindAllStatement(COLLECTION_NAME_1);
        assertThat(statement.toJs())
            .isEqualTo(statement.toString())
            .isEqualTo("db.collectionName.find({});");
    }
}