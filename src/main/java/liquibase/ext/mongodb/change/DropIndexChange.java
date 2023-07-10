package liquibase.ext.mongodb.change;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2020 Mastercard
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

import liquibase.change.ChangeMetaData;
import liquibase.change.CheckSum;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.mongodb.statement.DropIndexStatement;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@DatabaseChange(name = "dropIndex",
        description = "Drops index for a collection by keys" +
                "https://docs.mongodb.com/manual/reference/method/db.collection.dropIndex/#db.collection.dropIndex",
        priority = ChangeMetaData.PRIORITY_DATABASE, appliesTo = "collection")
@NoArgsConstructor
@Getter
@Setter
public class DropIndexChange extends AbstractMongoChange {

    private String collectionName;
    private String keys;

    @Override
    public String getConfirmationMessage() {
        return "Index dropped for collection " + getCollectionName();
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{
                new DropIndexStatement(collectionName, keys)
        };
    }

    @Override
    public CheckSum generateCheckSum() {
        return super.generateCheckSum(collectionName, keys);
    }

}
