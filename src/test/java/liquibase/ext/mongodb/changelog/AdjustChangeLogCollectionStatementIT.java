package liquibase.ext.mongodb.changelog;

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

class AdjustChangeLogCollectionStatementIT extends AbstractMongoIntegrationTest {

    public static final String LOG_COLLECTION_NAME = "historyLogCollection";


    @Test
    void executeToJSTest() {
        AdjustChangeLogCollectionStatement adjustChangeLogCollectionStatement =
                new AdjustChangeLogCollectionStatement(LOG_COLLECTION_NAME);

        assertThat(adjustChangeLogCollectionStatement.getCommandName()).isEqualTo("adjustChangeLogCollection");
        assertThat(adjustChangeLogCollectionStatement.getCollectionName()).isEqualTo(LOG_COLLECTION_NAME);
        assertThat(adjustChangeLogCollectionStatement.getSupportsValidator()).isTrue();
        assertThat(adjustChangeLogCollectionStatement.toJs()).isEqualTo("db.adjustChangeLogCollection({\"collMod\": \"historyLogCollection\", \"validator\": {\"$jsonSchema\": {\"bsonType\": \"object\", \"description\": \"Database Change Log Table.\", \"required\": [\"id\", \"author\", \"fileName\"], \"properties\": {\"id\": {\"bsonType\": \"string\", \"description\": \"Value from the changeSet id attribute.\"}, \"author\": {\"bsonType\": \"string\", \"description\": \"Value from the changeSet author attribute.\"}, \"fileName\": {\"bsonType\": \"string\", \"description\": \"Path to the changelog. This may be an absolute path or a relative path depending on how the changelog was passed to Liquibase. For best results, it should be a relative path.\"}, \"dateExecuted\": {\"bsonType\": \"date\", \"description\": \"Date/time of when the changeSet was executed. Used with orderExecuted to determine rollback order.\"}, \"orderExecuted\": {\"bsonType\": \"int\", \"description\": \"Order that the changeSets were executed. Used in addition to dateExecuted to ensure order is correct even when the databases datetime supports poor resolution.\"}, \"execType\": {\"bsonType\": \"string\", \"enum\": [\"EXECUTED\", \"FAILED\", \"SKIPPED\", \"RERAN\", \"MARK_RAN\"], \"description\": \"Description of how the changeSet was executed.\"}, \"md5sum\": {\"bsonType\": \"string\", \"description\": \"Checksum of the changeSet when it was executed. Used on each run to ensure there have been no unexpected changes to changSet in the changelog file.\"}, \"description\": {\"bsonType\": \"string\", \"description\": \"Short auto-generated human readable description of changeSet.\"}, \"comments\": {\"bsonType\": \"string\", \"description\": \"Value from the changeSet comment attribute.\"}, \"tag\": {\"bsonType\": \"string\", \"description\": \"Tracks which changeSets correspond to tag operations.\"}, \"contexts\": {\"bsonType\": \"string\", \"description\": \"Context expression of the run.\"}, \"labels\": {\"bsonType\": \"string\", \"description\": \"Labels assigned.\"}, \"deploymentId\": {\"bsonType\": \"string\", \"description\": \"Unique identifier generate for a run.\"}, \"liquibase\": {\"bsonType\": \"string\", \"description\": \"Version of Liquibase used to execute the changeSet.\"}}}}, \"validationAction\": \"error\", \"validationLevel\": \"strict\"});");
    }

    @Test
    void executeTest() {

        new CreateChangeLogCollectionStatement(LOG_COLLECTION_NAME).execute(connection);

        MongoCollection<Document> collection = connection.getDatabase().getCollection(LOG_COLLECTION_NAME);

        // options not present
        final Document collectionInfo =
                connection.getDatabase().listCollections().filter(Filters.eq("name", LOG_COLLECTION_NAME)).first();

        assertThat(collectionInfo)
                .isNotNull()
                .returns(TRUE, c -> ((Document) c.get("options")).isEmpty());

        // has just default index
        List<Document> indexes = new ArrayList<>();
        collection.listIndexes().into(indexes);
        assertThat(indexes).hasSize(1);
        assertThat(indexes.get(0).get("name")).isEqualTo("_id_");

        // with explicit supportsValidator=false should not change validators should add indexes only
        new AdjustChangeLogCollectionStatement(LOG_COLLECTION_NAME, FALSE).execute(connection);
        final Document collectionInfoExplicitNoAdjustment =
                connection.getDatabase().listCollections().filter(Filters.eq("name", LOG_COLLECTION_NAME)).first();

        assertThat(collectionInfoExplicitNoAdjustment)
                .isNotNull()
                .returns(TRUE, c -> ((Document) c.get("options")).isEmpty());

        indexes.clear();
        connection.getDatabase().getCollection(LOG_COLLECTION_NAME).listIndexes().into(indexes);
        assertThat(indexes).hasSize(2);
        assertThat(indexes.stream().filter(i -> i.get("name").equals("ui_" + LOG_COLLECTION_NAME)).findFirst().orElse(null))
                .isNotNull()
                .returns(TRUE, i -> i.get("unique"))
                .returns(1, i -> ((Document) i.get("key")).get("fileName"))
                .returns(1, i -> ((Document) i.get("key")).get("author"))
                .returns(1, i -> ((Document) i.get("key")).get("id"));

        // with explicit supportsValidator=true validator should be changed indexes remain same
        new AdjustChangeLogCollectionStatement(LOG_COLLECTION_NAME, TRUE).execute(connection);

        final Document collectionInfoAdjusted =
                connection.getDatabase().listCollections().filter(Filters.eq("name", LOG_COLLECTION_NAME)).first();

        assertThat(collectionInfoAdjusted)
                .isNotNull()
                .returns(FALSE, c -> ((Document) c.get("options")).isEmpty())
                .returns(FALSE, c -> ((Document) ((Document) c.get("options")).get("validator")).isEmpty())
                .returns("error", c -> ((Document) c.get("options")).get("validationAction"))
                .returns("strict", c -> ((Document) c.get("options")).get("validationLevel"));

        indexes.clear();
        connection.getDatabase().getCollection(LOG_COLLECTION_NAME).listIndexes().into(indexes);
        assertThat(indexes).hasSize(2);
        assertThat(indexes.stream().filter(i -> i.get("name").equals("ui_" + LOG_COLLECTION_NAME)).findFirst().orElse(null))
                .isNotNull();
    }
}