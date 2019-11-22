package liquibase.ext.mongodb.database;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import lombok.Getter;
import lombok.Setter;
import org.bson.UuidRepresentation;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.UuidCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

@Getter
@Setter
public class MongoConnection implements DatabaseConnection {

    public final String MONGO_CONNECTION_STRING_PATTERN = "%s/%s";

    private final MongoClient con;
    private final com.mongodb.client.MongoDatabase db;
    private final ConnectionString connectionString;

    public MongoConnection(final String connectionString) {
        this.connectionString = new ConnectionString(connectionString);
        this.con = MongoClients.create(this.connectionString);
        this.db = this.con.getDatabase(this.connectionString.getDatabase())
                .withCodecRegistry(uuidCodecRegistry());
    }

    public static CodecRegistry uuidCodecRegistry() {
        return CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new UuidCodec(UuidRepresentation.STANDARD))
                //, fromProviders(PojoCodecProvider.builder().automatic(true).build())
                , MongoClientSettings.getDefaultCodecRegistry()
        );
    }

    public static CodecRegistry pojoCodecRegistry() {
        return CodecRegistries.fromRegistries(
                fromProviders(new UuidCodecProvider(UuidRepresentation.STANDARD))
                , fromProviders(PojoCodecProvider.builder().automatic(true).build())
                , MongoClientSettings.getDefaultCodecRegistry()
        );
    }

    public void close() throws DatabaseException {
        try {
            con.close();
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    public void commit() throws DatabaseException {
        // NA
    }

    public boolean getAutoCommit() throws DatabaseException {
        return false;
    }

    public void setAutoCommit(boolean autoCommit) throws DatabaseException {

    }

    @Override
    public String getCatalog() throws DatabaseException {
        return this.connectionString.getDatabase();
    }

    public String nativeSQL(String sql) throws DatabaseException {
        return null;
    }

    public void rollback() throws DatabaseException {
        // NA
    }

    public String getDatabaseProductName() throws DatabaseException {
        //TODO: refer to metadata

        return MongoLiquibaseDatabase.MONGODB_PRODUCT_NAME;
    }

    public String getDatabaseProductVersion() throws DatabaseException {
        return null;
    }

    public int getDatabaseMajorVersion() throws DatabaseException {
        return 0;
    }

    public int getDatabaseMinorVersion() throws DatabaseException {
        return 0;
    }

    @Override
    public String getURL() {
        return String.join(",", this.connectionString.getHosts());
    }

    public String getConnectionUserName() {
        return null;
    }

    public boolean isClosed() throws DatabaseException {
        return false;
    }

    public void attached(Database database) {

    }

}
