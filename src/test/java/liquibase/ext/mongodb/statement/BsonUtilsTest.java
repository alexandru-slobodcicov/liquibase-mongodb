package liquibase.ext.mongodb.statement;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

class BsonUtilsTest {

    @Test
    void orEmptyDocument() {
        assertThat(BsonUtils.orEmptyDocument("{code: UUID(\"fe4f70d0-d08d-86d0-6147-8d279a4fde9d\")}").get("code", UUID.class))
            .isEqualTo(UUID.fromString("fe4f70d0-d08d-86d0-6147-8d279a4fde9d"));
        assertThat(BsonUtils.orEmptyDocument(null).isEmpty()).isTrue();
        assertThat(BsonUtils.orEmptyDocument("").isEmpty()).isTrue();
        assertThat(BsonUtils.orEmptyDocument("{id:1}").isEmpty()).isFalse();
    }

    @Test
    void orEmptyDocumentSpecialChars() {
        assertThat(BsonUtils.orEmptyDocument("{name: \"Bank Złoto\"}").getString("name")).isEqualTo("Bank Złoto");
    }

    @Test
    void orEmptyArray() {
        assertThat(
            BsonUtils.orEmptyList("[{code: UUID(\"fe4f70d0-d08d-86d0-6147-8d279a4fde9d\")}]")
                .stream()
                .map(d -> d.get("code"))
                .collect(toList())).contains(UUID.fromString("fe4f70d0-d08d-86d0-6147-8d279a4fde9d"));
        assertThat(BsonUtils.orEmptyList(null)).isEmpty();
        assertThat(BsonUtils.orEmptyList("")).isEmpty();
        assertThat(BsonUtils.orEmptyList("[{id:1}, {id:2}]")).hasSize(2);
    }
}