package liquibase.ext.mongodb.lockservice;

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

    public static final String COMMAND_NAME = "update";

    protected static final String HOST_NAME;
    protected static final String HOST_ADDRESS;
    protected static final String HOST_DESCRIPTION = (System.getProperty("liquibase.hostDescription") == null) ? "" :
            ("#" + System.getProperty("liquibase.hostDescription"));

    static {
        try {
            HOST_NAME = NetUtil.getLocalHostName();
            HOST_ADDRESS = NetUtil.getLocalHostAddress();
        } catch (Exception e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }

    private String collectionName;
    private boolean locked;

    @Override
    public String toJs() {
        //TODO: Adjust and unit test
        return "db." +
                collectionName +
                "." +
                COMMAND_NAME +
                "(" +
                ");";
    }

    @Override
    public int update(MongoDatabase db) {

        final MongoChangeLogLock entry = new MongoChangeLogLock(1, new Date()
                , HOST_NAME + HOST_DESCRIPTION + " (" + HOST_ADDRESS + ")", true);
        final Document inputDocument = entry.toDocument();
        inputDocument.put("locked", locked);
        final Optional<Document> changeLogLock = Optional.ofNullable(
                db.getCollection(collectionName)
                        .findOneAndReplace(Filters.eq("_id", entry.getId()), inputDocument, new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER))
        );
        return changeLogLock.map(e -> 1).orElse(0);
    }


}
