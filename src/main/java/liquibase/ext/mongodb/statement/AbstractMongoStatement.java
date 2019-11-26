package liquibase.ext.mongodb.statement;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2019 Mastercard
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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
