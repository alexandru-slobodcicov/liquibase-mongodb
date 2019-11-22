package liquibase.ext.mongodb.statement;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class RunCommandStatementTest extends AbstractMongoIntegrationTest {

    private static final String INSERT_CMD = ""
        + "{\n"
        + "   insert: \"" + COLLECTION_NAME_1 + "\",\n"
        + "   documents: [ {user:1},{user:2}],\n"
        + "   ordered: true,\n"
        + "   writeConcern: { w: \"majority\", wtimeout: 5000 },\n"
        + "   bypassDocumentValidation: true\n"
        + "}";

    @Test
    void runFromString() {
        final MongoDatabase database = mongoConnection.getDb();
        new RunCommandStatement(INSERT_CMD).execute(database);
        final FindIterable<Document> docs = database.getCollection(COLLECTION_NAME_1).find();
        assertThat(docs).hasSize(2);
        assertThat(docs.iterator().next())
            .containsEntry("user", 1)
            .containsKey("_id");
    }

    @Test
    void runFromDocument() {
        final MongoDatabase database = mongoConnection.getDb();
        new RunCommandStatement(Document.parse(INSERT_CMD)).execute(database);
        final FindIterable<Document> docs = database.getCollection(COLLECTION_NAME_1).find();
        assertThat(docs).hasSize(2);
        assertThat(docs.iterator().next())
            .containsEntry("user", 1)
            .containsKey("_id");
    }

    @Test
    void toStringJs() {
        final RunCommandStatement statement = new RunCommandStatement(INSERT_CMD);
        assertThat(statement.toJs())
            .isEqualTo(statement.toString())
            .isEqualTo("db.runCommand({\"insert\": \"collectionName\", \"documents\": [{\"user\": 1}, {\"user\": 2}], \"ordered\": true, "
                + "\"writeConcern\": {\"w\": \"majority\", \"wtimeout\": 5000}, \"bypassDocumentValidation\": true});");
    }
}