package liquibase.ext.mongodb.lockservice;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import liquibase.ext.mongodb.database.MongoConnection;
import liquibase.ext.mongodb.statement.AbstractMongoStatement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class SelectLockChangeLogStatement extends AbstractMongoStatement {


    public static String COMMAND = "findOne";

    public String collectionName;

    @Override
    public String toJs() {
        //TODO: Adjust and unit test
        return new StringBuilder()
                .append("db.")
                .append(collectionName)
                .append(".")
                .append(COMMAND)
                .append("(")
                .append(");")
                .toString();
    }

    @Override
    public <LockEntry> LockEntry queryForObject(final MongoDatabase db, final Class<LockEntry> requiredType) {

        final LockEntry entry =
                db.getCollection(collectionName, requiredType).withCodecRegistry(MongoConnection.pojoCodecRegistry())
                        .find(Filters.eq("id", 1)).first();
    return entry;
    }
}