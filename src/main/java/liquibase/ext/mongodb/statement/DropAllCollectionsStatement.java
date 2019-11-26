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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import java.util.function.Consumer;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(callSuper = true)
public class DropAllCollectionsStatement extends AbstractMongoStatement {

    public static final String COMMAND = "dropAll";

    @Override
    public String toJs() {
        return
                new StringBuilder()
                        .append("db.")
                        .append(COMMAND)
                        .append("(")
                        .append(");")
                        .toString();
    }

    @Override
    public void execute(MongoDatabase db) {
        db.listCollectionNames()
            .map(db::getCollection)
            .forEach((Consumer<? super MongoCollection<Document>>) MongoCollection::drop);
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}
