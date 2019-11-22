package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.assertj.core.api.SoftAssertions;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.stream.StreamSupport;

import static liquibase.ext.mongodb.TestUtils.COLLECTION_NAME_1;
import static org.assertj.core.api.Assertions.assertThat;

class CreateIndexStatementTest extends AbstractMongoIntegrationTest {

    @Test
    void toStringJs() {
        final String indexName = "locale_indx";
        final CreateIndexStatement createIndexStatement = new CreateIndexStatement(COLLECTION_NAME_1, "{ locale: 1 }",
            "{ name: \"" + indexName + "\", unique: true}");
        assertThat(createIndexStatement.toString())
            .isEqualTo(createIndexStatement.toJs())
            .isEqualTo("db.collectionName. createIndex({\"locale\": 1}, {\"name\": \"locale_indx\", \"unique\": true});");
    }

    @Test
    void execute() {
        final MongoDatabase database = mongoConnection.getDb();
        final Document initialDocument = Document.parse("{name: \"test name\", surname: \"test surname\", locale: \"EN\"}");
        final String indexName = "locale_indx";
        database.createCollection(COLLECTION_NAME_1);
        database.getCollection(COLLECTION_NAME_1).insertOne(initialDocument);
        final CreateIndexStatement createIndexStatement = new CreateIndexStatement(COLLECTION_NAME_1, "{ locale: 1 }",
            "{ name: \"" + indexName + "\", unique: true}");
        createIndexStatement.execute(database);

        final Document document = StreamSupport.stream(database.getCollection(COLLECTION_NAME_1).listIndexes().spliterator(), false)
            .filter(doc -> doc.get("name").equals(indexName))
            .findAny()
            .orElseThrow(() -> new IllegalStateException("Index not found"));

        SoftAssertions.assertSoftly(soflty -> {
            soflty.assertThat(document.get("unique")).isEqualTo(true);
            soflty.assertThat(document.get("key")).isEqualTo(Document.parse("{ locale: 1 }"));
        });
    }
}