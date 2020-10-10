package liquibase.ext.mongodb.lockservice;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThat;

class AdjustChangeLogLockCollectionStatementIT extends AbstractMongoIntegrationTest {

    public static final String LOCK_COLLECTION_NAME = "lockCollection";


    @Test
    void executeToJSTest() {
        AdjustChangeLogLockCollectionStatement adjustChangeLogLockCollectionStatement =
                new AdjustChangeLogLockCollectionStatement(LOCK_COLLECTION_NAME);

        assertThat(adjustChangeLogLockCollectionStatement.getCommandName()).isEqualTo("adjustChangeLogLockCollection");
        assertThat(adjustChangeLogLockCollectionStatement.getCollectionName()).isEqualTo(LOCK_COLLECTION_NAME);
        assertThat(adjustChangeLogLockCollectionStatement.getSupportsValidator()).isTrue();
        assertThat(adjustChangeLogLockCollectionStatement.toJs()).isEqualTo("db.adjustChangeLogLockCollection({\"collMod\": \"lockCollection\", \"validator\": {\"$jsonSchema\": {\"bsonType\": \"object\", \"description\": \"Database Lock Collection\", \"required\": [\"_id\", \"locked\"], \"properties\": {\"_id\": {\"bsonType\": \"int\", \"description\": \"Unique lock identifier\"}, \"locked\": {\"bsonType\": \"bool\", \"description\": \"Lock flag\"}, \"lockGranted\": {\"bsonType\": \"date\", \"description\": \"Timestamp when lock acquired\"}, \"lockedBy\": {\"bsonType\": \"string\", \"description\": \"Owner of the lock\"}}}}, \"validationAction\": \"error\", \"validationLevel\": \"strict\"});");
    }

    @Test
    void executeTest() {

        new CreateChangeLogLockCollectionStatement(LOCK_COLLECTION_NAME).execute(connection);

        MongoCollection<Document> collection = connection.getDatabase().getCollection(LOCK_COLLECTION_NAME);

        List<Document> indexes = new ArrayList<>();

        collection.listIndexes().into(indexes);

        // has just default index
        assertThat(indexes).hasSize(1);
        assertThat(indexes.get(0).get("name")).isEqualTo("_id_");

        // options not present
        final Document collectionInfo =
                connection.getDatabase().listCollections().filter(Filters.eq("name", LOCK_COLLECTION_NAME)).first();

        assertThat(collectionInfo)
                .isNotNull()
                .returns(TRUE, c -> ((Document) c.get("options")).isEmpty());

        // with explicit supportsValidator=false should not be changed
        new AdjustChangeLogLockCollectionStatement(LOCK_COLLECTION_NAME, FALSE).execute(connection);
        final Document collectionInfoExplicitNoAdjustment =
                connection.getDatabase().listCollections().filter(Filters.eq("name", LOCK_COLLECTION_NAME)).first();

        assertThat(collectionInfoExplicitNoAdjustment)
                .isNotNull()
                .returns(TRUE, c -> ((Document) c.get("options")).isEmpty());

        // with explicit supportsValidator=true should be changed
        new AdjustChangeLogLockCollectionStatement(LOCK_COLLECTION_NAME, TRUE).execute(connection);

        indexes.clear();
        connection.getDatabase().getCollection(LOCK_COLLECTION_NAME).listIndexes().into(indexes);
        assertThat(indexes).hasSize(1);
        assertThat(indexes.get(0).get("name")).isEqualTo("_id_");

        final Document collectionInfoAdjusted =
                connection.getDatabase().listCollections().filter(Filters.eq("name", LOCK_COLLECTION_NAME)).first();

        assertThat(collectionInfoAdjusted)
                .isNotNull()
                .returns(FALSE, c -> ((Document) c.get("options")).isEmpty())
                .returns(FALSE, c -> ((Document) ((Document) c.get("options")).get("validator")).isEmpty())
                .returns("error", c -> ((Document) c.get("options")).get("validationAction"))
                .returns("strict", c -> ((Document) c.get("options")).get("validationLevel"));
    }
}