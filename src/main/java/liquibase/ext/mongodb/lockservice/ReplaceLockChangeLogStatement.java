package liquibase.ext.mongodb.lockservice;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.mongodb.statement.AbstractMongoStatement;
import liquibase.util.NetUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.bson.Document;

import java.util.Date;
import java.util.Optional;

@AllArgsConstructor
@Getter
@Setter
public class ReplaceLockChangeLogStatement extends AbstractMongoStatement {

    protected static final String hostName;
    protected static final String hostAddress;
    protected static final String hostDescription = (System.getProperty("liquibase.hostDescription") == null) ? "" :
            ("#" + System.getProperty("liquibase.hostDescription"));
    public static String COMMAND = "update";

    static {
        try {
            hostName = NetUtil.getLocalHostName();
            hostAddress = NetUtil.getLocalHostAddress();
        } catch (Exception e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }

    public String collectionName;
    public boolean locked;

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
    public int update(MongoDatabase db) {

        final MongoChangeLogLock entry = new MongoChangeLogLock(1, new Date()
                , hostName + hostDescription + " (" + hostAddress + ")", true);
        final Document inputDocument = entry.toDocument();
        inputDocument.put("locked", locked);
        final Optional<Document> changeLogLock = Optional.ofNullable(
                db.getCollection(collectionName)
                        .findOneAndReplace(Filters.eq("_id", entry.getId()), inputDocument, new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER))
        );
        return changeLogLock.map(e -> 1).orElse(0);
    }


}