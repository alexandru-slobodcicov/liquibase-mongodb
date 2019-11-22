package liquibase.ext.mongodb.statement;

import com.mongodb.client.MongoDatabase;
import liquibase.exception.DatabaseException;
import liquibase.statement.AbstractSqlStatement;

import java.util.List;

public abstract class AbstractMongoStatement extends AbstractSqlStatement {

    @Override
    public boolean continueOnError() {
        return false;
    }

    @Override
    public boolean skipOnUnsupported() {
        return false;
    }

    public abstract String toJs();

    public void execute(final MongoDatabase db) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    public long queryForLong(final MongoDatabase db) {
        throw new UnsupportedOperationException();
    }

    public int update(final MongoDatabase db) {
        throw new UnsupportedOperationException();
    }

    public <T> T queryForObject(final MongoDatabase db, final Class<T> requiredType) {
        throw new UnsupportedOperationException();
    }

    public List queryForList(final MongoDatabase db, final Class elementType) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

}
