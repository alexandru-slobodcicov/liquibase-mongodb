package liquibase.ext.mongodb.lockservice;

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
