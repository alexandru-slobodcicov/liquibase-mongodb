package liquibase.ext.mongodb.changelog.codec;

import liquibase.changelog.RanChangeSet;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

//TODO: Implement this or use POJO default codec
public class RanChangeSetCodec implements Codec<RanChangeSet> {

    public RanChangeSetCodec() {
        super();
    }

    @Override
    public RanChangeSet decode(final BsonReader bsonReader, final DecoderContext decoderContext) {
        return null;
    }

    @Override
    public void encode(final BsonWriter bsonWriter, final RanChangeSet changeSet, final EncoderContext encoderContext) {
        bsonWriter.writeStartDocument();


        bsonWriter.writeEndDocument();
    }

    @Override
    public Class<RanChangeSet> getEncoderClass() {
        return RanChangeSet.class;
    }
}
