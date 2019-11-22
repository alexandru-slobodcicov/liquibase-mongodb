package liquibase.ext.mongodb.executor;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.mongodb.statement.AbstractMongoStatement;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

public class MongoSqlGenerator extends AbstractSqlGenerator<AbstractMongoStatement> {

    @Override
    public ValidationErrors validate(AbstractMongoStatement statement, Database database,
                                     SqlGeneratorChain<AbstractMongoStatement> sqlGeneratorChain) {
        return null;
    }

    @Override
    public Sql[] generateSql(AbstractMongoStatement statement, Database database, SqlGeneratorChain<AbstractMongoStatement> sqlGeneratorChain) {
        return new Sql[0];
    }

    @Override
    public boolean generateStatementsIsVolatile(Database database) {
        return true;
    }
}
