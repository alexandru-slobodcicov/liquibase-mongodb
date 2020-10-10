package liquibase.ext.mongodb.database;

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

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import liquibase.ext.mongodb.statement.BsonUtils;
import liquibase.nosql.database.AbstractNoSqlConnection;
import liquibase.exception.DatabaseException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.Driver;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

import static java.util.Optional.ofNullable;

@Getter
@Setter
@NoArgsConstructor
public class MongoConnection extends AbstractNoSqlConnection {

    public static final int DEFAULT_PORT = 27017;
    public static final String MONGO_PREFIX = MongoLiquibaseDatabase.MONGODB_PRODUCT_SHORT_NAME + "://";
    public final String MONGO_CONNECTION_STRING_PATTERN = "%s/%s";

    private ConnectionString connectionString;

    protected MongoClient client;

    protected MongoDatabase database;

    @Override
    public String getCatalog() throws DatabaseException {
        try {
            return database.getName();
        } catch (final Exception e) {
            throw new DatabaseException(e);
        }
    }

    public String getDatabaseProductName() throws DatabaseException {
        return MongoLiquibaseDatabase.MONGODB_PRODUCT_NAME;
    }

    @Override
    public String getURL() {
        return String.join(",", ofNullable(this.connectionString).map(ConnectionString::getHosts).orElse(Collections.emptyList()));
    }

    @Override
    public void open(final String url, final Driver driverObject, final Properties driverProperties) throws DatabaseException {

        try {
            this.connectionString = new ConnectionString(url);
            this.client = MongoClients.create(this.connectionString);
            this.database = this.client.getDatabase(Objects.requireNonNull(this.connectionString.getDatabase()))
                    .withCodecRegistry(BsonUtils.uuidCodecRegistry());
        } catch (final Exception e) {
            throw new DatabaseException("Could not open connection to database: " + connectionString.getDatabase(), e);
        }
    }

    @Override
    public void close() throws DatabaseException {
        try {
            client.close();
        } catch (final Exception e) {
            throw new DatabaseException(e);
        }
    }


}
