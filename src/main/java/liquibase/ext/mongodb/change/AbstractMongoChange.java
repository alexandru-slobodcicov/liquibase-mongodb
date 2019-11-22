package liquibase.ext.mongodb.change;

import liquibase.change.AbstractChange;
import liquibase.database.Database;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;

public abstract class AbstractMongoChange extends AbstractChange {

    @Override
    public boolean supports(Database database) {
        return database instanceof MongoLiquibaseDatabase;
    }
}
