package org.bson.codecs;

import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.UUID;

public class Uuid4CodecProvider extends UuidCodecProvider {
    private UuidRepresentation uuidRepresentation;

    /**
     * Set the UUIDRepresentation to be used in the codec
     * default is JAVA_LEGACY to be compatible with existing documents
     *
     * @param uuidRepresentation the representation of UUID
     * @see UuidRepresentation
     * @since 3.0
     */
    public Uuid4CodecProvider(UuidRepresentation uuidRepresentation) {
        super(uuidRepresentation);
        this.uuidRepresentation = uuidRepresentation;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if (clazz == UUID.class) {
            return (Codec<T>) (new Uuid4Codec(uuidRepresentation));
        }
        return null;
    }
}
