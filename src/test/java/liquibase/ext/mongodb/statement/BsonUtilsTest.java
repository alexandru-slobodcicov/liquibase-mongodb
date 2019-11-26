package liquibase.ext.mongodb.statement;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2019 Mastercard
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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
