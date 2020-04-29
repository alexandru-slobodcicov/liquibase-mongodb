package org.bson.internal;

import org.bson.BsonBinarySubType;
import org.bson.BsonSerializationException;
import org.bson.UuidRepresentation;

import java.util.UUID;

public class Uuid4Helper {
    public static byte[] encodeUuidToBinary(UUID uuid, UuidRepresentation uuidRepresentation) {
        return UuidHelper.encodeUuidToBinary(uuid, uuidRepresentation);
    }

    public static UUID decodeBinaryToUuid(byte[] data, byte type, UuidRepresentation uuidRepresentation) {
        if (data.length != 16) {
            throw new BsonSerializationException(String.format("Expected length to be 16, not %d.", data.length));
        } else {
            if (type == BsonBinarySubType.UUID_LEGACY.getValue()) {
                switch (uuidRepresentation) {
                    case STANDARD:
                        break;
                    default:
                        return UuidHelper.decodeBinaryToUuid(data, type, uuidRepresentation);
                }
            }

            return new UUID(readLongFromArrayBigEndian(data, 0), readLongFromArrayBigEndian(data, 8));
        }
    }

    private static long readLongFromArrayBigEndian(byte[] bytes, int offset) {
        long x = 0L;
        x |= 255L & (long) bytes[offset + 7];
        x |= (255L & (long) bytes[offset + 6]) << 8;
        x |= (255L & (long) bytes[offset + 5]) << 16;
        x |= (255L & (long) bytes[offset + 4]) << 24;
        x |= (255L & (long) bytes[offset + 3]) << 32;
        x |= (255L & (long) bytes[offset + 2]) << 40;
        x |= (255L & (long) bytes[offset + 1]) << 48;
        x |= (255L & (long) bytes[offset]) << 56;
        return x;
    }
}
