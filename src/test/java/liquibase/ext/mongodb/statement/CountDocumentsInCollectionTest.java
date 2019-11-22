package liquibase.ext.mongodb.statement;

import liquibase.ext.mongodb.TestUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CountDocumentsInCollectionTest {
    private static final String COLLECTION_NAME = TestUtils.COLLECTION_NAME_1;
    private static final String COLLECTION_CMD = String.format("db.countDocumentsInCollection(%s);", COLLECTION_NAME);
    private static final CountDocumentsInCollection COUNT_COLLECTION = new CountDocumentsInCollection(COLLECTION_NAME);

    @Test
    void toJs() {
        assertThat(COUNT_COLLECTION.toJs())
            .isEqualTo(COLLECTION_CMD);
    }

    @Test
    void shouldReturnToString() {
        assertThat(COUNT_COLLECTION.toJs())
            .isEqualTo(COUNT_COLLECTION.toString())
            .isEqualTo(COLLECTION_CMD, COLLECTION_NAME);
    }
}