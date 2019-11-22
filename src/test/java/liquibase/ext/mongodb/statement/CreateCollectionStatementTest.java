package liquibase.ext.mongodb.statement;

import liquibase.ext.AbstractMongoIntegrationTest;
import org.junit.jupiter.api.Test;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static liquibase.ext.mongodb.TestUtils.EMPTY_OPTION;
import static liquibase.ext.mongodb.TestUtils.OPTION_1;
import static liquibase.ext.mongodb.TestUtils.getCollections;
import static org.assertj.core.api.Assertions.assertThat;

class CreateCollectionStatementTest extends AbstractMongoIntegrationTest {

    private static final String EXPECTED_COMMAND = "db.createCollection(collectionName, {\"opt1\": \"option 1\"});";

    @Test
    void execute() {
        final String collection = COLLECTION_NAME_1 + System.nanoTime();
        final CreateCollectionStatement statement = new CreateCollectionStatement(collection, EMPTY_OPTION);
        statement.execute(mongoConnection.getDb());
        assertThat(getCollections(mongoConnection))
            .contains(collection);
    }

    @Test
    void toStringJs() {
        final CreateCollectionStatement statement = new CreateCollectionStatement(COLLECTION_NAME_1, OPTION_1);
        assertThat(statement.toJs())
            .isEqualTo(EXPECTED_COMMAND)
            .isEqualTo(statement.toString());
    }
}