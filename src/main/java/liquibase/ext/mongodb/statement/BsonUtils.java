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

import com.mongodb.DBRefCodecProvider;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.UuidCodecProvider;
import org.bson.codecs.ValueCodecProvider;

import java.util.ArrayList;
import java.util.List;

import static java.util.Optional.ofNullable;
import static liquibase.util.StringUtils.trimToNull;
import static lombok.AccessLevel.PRIVATE;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

@NoArgsConstructor(access = PRIVATE)
public final class BsonUtils {

    public static final DocumentCodec DOCUMENT_CODEC =
        new DocumentCodec(fromProviders(
            new UuidCodecProvider(UuidRepresentation.PYTHON_LEGACY),
            new ValueCodecProvider(),
            new BsonValueCodecProvider(),
            new DocumentCodecProvider(),
            new DBRefCodecProvider()));

    public static Document orEmptyDocument(final String json) {
        return (
            ofNullable(trimToNull(json))
                .map(s -> Document.parse(s, DOCUMENT_CODEC))
                .orElseGet(Document::new)
        );
    }

    public static List<Document> orEmptyList(final String json) {
        return (
            ofNullable(trimToNull(json))
                .map(jn -> "{ items: " + jn + "}")
                .map(s -> Document.parse(s, DOCUMENT_CODEC))
                .map(d -> d.getList("items", Document.class, new ArrayList<>()))
                .orElseGet(ArrayList::new)
        );
    }
}
