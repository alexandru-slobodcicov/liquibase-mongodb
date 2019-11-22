package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.junit.jupiter.api.Test;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class DropCollectionStatementTest extends AbstractMongoIntegrationTest {

    @Test
    void execute() {
        final MongoDatabase database = mongoConnection.getDb();
        database.createCollection(COLLECTION_NAME_1);
        assertThat(database.listCollectionNames()).hasSize(1);

        new DropCollectionStatement(COLLECTION_NAME_1).execute(database);
        assertThat(database.listCollectionNames()).isEmpty();
    }

    @Test
    void toString1() {
        final DropCollectionStatement statement = new DropCollectionStatement(COLLECTION_NAME_1);
        assertThat(statement.toString())
            .isEqualTo(statement.toJs())
            .isEqualTo("db.collectionName.drop();");
    }
}