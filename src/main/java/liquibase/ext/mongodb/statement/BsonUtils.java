package liquibase.ext.mongodb.statement;

import com.mongodb.DBRefCodecProvider;
import liquibase.util.StringUtils;
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
            new UuidCodecProvider(UuidRepresentation.STANDARD),
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
