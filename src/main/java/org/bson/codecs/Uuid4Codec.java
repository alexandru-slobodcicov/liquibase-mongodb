package org.bson.codecs;

import org.bson.BSONException;
import org.bson.BsonBinarySubType;
import org.bson.BsonReader;
import org.bson.UuidRepresentation;
import org.bson.internal.Uuid4Helper;

import java.util.UUID;

public class Uuid4Codec extends UuidCodec {

    private final UuidRepresentation uuidRepresentation;

    public Uuid4Codec(UuidRepresentation uuidRepresentation) {
        super(uuidRepresentation);
        this.uuidRepresentation = uuidRepresentation;
    }

    @Override
    public UUID decode(BsonReader reader, DecoderContext decoderContext) {
        byte subType = reader.peekBinarySubType();
        if (subType != BsonBinarySubType.UUID_LEGACY.getValue() && subType != BsonBinarySubType.UUID_STANDARD.getValue()) {
            throw new BSONException("Unexpected BsonBinarySubType");
        } else {
            byte[] bytes = reader.readBinaryData().getData();
            return Uuid4Helper.decodeBinaryToUuid(bytes, subType, this.uuidRepresentation);
        }
    }
}
