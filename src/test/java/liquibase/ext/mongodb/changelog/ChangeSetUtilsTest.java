package liquibase.ext.mongodb.changelog;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ChangeSetUtilsTest {

    private final String json = "{\"key1\": \"value1\", \"key2\": \"value2\", \"ChildKey\": {\"key3\": \"value3\"}}";

    @Test
    void toDocument() {
        final Document document = Document.parse(json);
        assertThat(document)
            .hasSize(3)
            .containsEntry("key1", "value1")
            .containsEntry("key2", "value2")
            .containsEntry("ChildKey", new Document("key3", "value3"));
    }

    @Test
    void fromDocument() {
        final Map<String, Object> docMap = new HashMap<>();
        docMap.put("key1", "value1");
        docMap.put("key2", "value2");
        docMap.put("ChildKey", Collections.singletonMap("key3", "value3"));
        final Document document = new Document(docMap);
        assertThat(document.toJson()).isEqualTo(json);
    }
}