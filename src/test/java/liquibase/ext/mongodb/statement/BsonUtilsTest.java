package liquibase.ext.mongodb.statement;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2019 Mastercard
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static java.lang.String.format;
import static java.util.UUID.fromString;
import static java.util.stream.Collectors.toList;
import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;
import static org.assertj.core.api.Assertions.assertThat;

class BsonUtilsTest {
    final String UUID_V4 = "e4408c36-4661-4bb2-8565-3e8187f34849";

    @Test
    void shouldProcessProperlyUUID() {
        final UUID uuid = fromString(UUID_V4);
        assertThat(uuid.version()).isEqualTo(4);
        assertThat(uuid.toString()).isEqualTo(UUID_V4);
    }

    @Test
    void shouldParseJsonAsDocument() {
        final Document uuidDoc = orEmptyDocument(format("{code: UUID(\"%s\")}", UUID_V4));
        final UUID uuidActual = uuidDoc.get("code", UUID.class);
        assertThat(uuidActual).hasToString(UUID_V4);

        assertThat(orEmptyDocument(null)).isEmpty();
        assertThat(orEmptyDocument("")).isEmpty();
        assertThat(orEmptyDocument("{id:1}")).isNotEmpty();
    }

    @Test
    void orEmptyDocumentSpecialChars() {
        assertThat(orEmptyDocument("{name: \"Bank Złoto\"}").getString("name")).isEqualTo("Bank Złoto");
    }

    @Test
    void orEmptyArray() {
        assertThat(
                BsonUtils.orEmptyList(format("[{code: UUID(\"%s\")}]", UUID_V4))
                        .stream()
                        .map(d -> d.get("code"))
                        .collect(toList())).contains(fromString(UUID_V4));
        assertThat(BsonUtils.orEmptyList(null)).isEmpty();
        assertThat(BsonUtils.orEmptyList("")).isEmpty();
        assertThat(BsonUtils.orEmptyList("[{id:1}, {id:2}]")).hasSize(2);
    }
}
