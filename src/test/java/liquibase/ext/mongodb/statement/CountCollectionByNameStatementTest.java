package liquibase.ext.mongodb.statement;

import liquibase.ext.AbstractMongoIntegrationTest;
import liquibase.ext.mongodb.TestUtils;
import org.junit.jupiter.api.Test;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class CountCollectionByNameStatementTest extends AbstractMongoIntegrationTest {
    private static final String COLLECTION_NAME = TestUtils.COLLECTION_NAME_1;
    private static final String COLLECTION_CMD = String.format("db.listCollectionNames(%s);", COLLECTION_NAME);
    private static final CountCollectionByNameStatement COUNT_COLLECTION = new CountCollectionByNameStatement(COLLECTION_NAME);

    @Test
    void queryForLong() {
        mongoExecutor.getDb().createCollection(COLLECTION_NAME_1);
        assertThat(new CountCollectionByNameStatement(COLLECTION_NAME_1).queryForLong(mongoConnection.getDb()))
            .isEqualTo(1);
    }

    @Test
    void shouldReturnToString() {
        assertThat(COUNT_COLLECTION.toJs())
            .isEqualTo(COUNT_COLLECTION.toString())
            .isEqualTo(COLLECTION_CMD, COLLECTION_NAME);
    }
}