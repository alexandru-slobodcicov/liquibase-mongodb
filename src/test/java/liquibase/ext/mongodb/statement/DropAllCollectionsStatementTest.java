package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class DropAllCollectionsStatementTest extends AbstractMongoIntegrationTest {

    @Test
    void execute() {
        final MongoDatabase database = mongoConnection.getDb();

        IntStream.rangeClosed(1, 5)
            .forEach(indx -> database.createCollection(COLLECTION_NAME_1 + indx));
        assertThat(database.listCollectionNames()).hasSize(5);

        new DropAllCollectionsStatement().execute(database);
        assertThat(database.listCollectionNames()).isEmpty();
    }

    @Test
    void toString1() {
        final DropAllCollectionsStatement statement = new DropAllCollectionsStatement();
        assertThat(statement.toJs())
            .isEqualTo(statement.toString())
            .isEqualTo("db.dropAll();");
    }
}