package liquibase.ext;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2019 Mastercard
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import liquibase.Scope;
import liquibase.database.DatabaseFactory;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.mongodb.TestUtils;
import liquibase.ext.mongodb.database.MongoConnection;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.ext.mongodb.executor.MongoExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.function.Consumer;

import static liquibase.ext.mongodb.TestUtils.getMongoConnection;

public abstract class AbstractMongoIntegrationTest {

    protected static MongoConnection mongoConnection;
    protected static MongoExecutor mongoExecutor;
    protected static MongoLiquibaseDatabase database;

    @AfterAll
    protected static void destroy() {
        database.getConnection().getDb().listCollectionNames()
            .forEach((Consumer<? super String>) c -> mongoConnection.getDb().getCollection(c).drop());
    }

    @BeforeEach
    protected void setUp() throws DatabaseException {

        mongoConnection = getMongoConnection("application-test.properties");

        //Can be achieved by excluding the package to scan or pass package list via system.parameter
        //ServiceLocator.getInstance().getPackages().remove("liquibase.executor");
        //Another way is to register the executor against a Db

        database = (MongoLiquibaseDatabase) DatabaseFactory.getInstance().findCorrectDatabaseImplementation(mongoConnection);
        database.setConnection(mongoConnection);
        Scope.getCurrentScope().getLog(TestUtils.class).fine("database is initialized...");

        mongoExecutor = new MongoExecutor();
        mongoExecutor.setDatabase(database);
        Scope.getCurrentScope().getLog(TestUtils.class).fine("mongoExecutor is initialized...");

        Scope.getCurrentScope().getSingleton(ExecutorService.class).setExecutor(database, mongoExecutor);

        database.getConnection().getDb().listCollectionNames()
            .forEach((Consumer<? super String>) c -> mongoConnection.getDb().getCollection(c).drop());
    }
}
