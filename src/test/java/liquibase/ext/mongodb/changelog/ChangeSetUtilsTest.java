package liquibase.ext.mongodb.changelog;

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
