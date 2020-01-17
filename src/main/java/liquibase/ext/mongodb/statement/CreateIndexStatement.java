package liquibase.ext.mongodb.statement;

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
import com.mongodb.client.model.IndexOptions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;

@Getter
@EqualsAndHashCode(callSuper = true)
public class CreateIndexStatement extends AbstractMongoStatement {

    public static final String COMMAND_NAME = "createIndex";

    private final String collectionName;
    private final Document keys;
    private final Document options;

    public CreateIndexStatement(final String collectionName, final Document keys, final Document options) {
        this.collectionName = collectionName;
        this.keys = keys;
        this.options = options;
    }

    public CreateIndexStatement(final String collectionName, final String keys, final String options) {
        this(collectionName, orEmptyDocument(keys), orEmptyDocument(options));
    }

    @Override
    public String toJs() {
        return
                "db."
                        + collectionName
                        + ". "
                        + COMMAND_NAME
                        + "("
                        + keys.toJson()
                        + ", "
                        + options.toJson()
                        + ");";
    }

    @Override
    public void execute(MongoDatabase db) {
        db.getCollection(collectionName).createIndex(keys, createIndexOptions());
    }

    private IndexOptions createIndexOptions() {
        //TODO: add POJO codec
        final IndexOptions indexOptions = new IndexOptions();
        if (options.containsKey("unique") && options.getBoolean("unique")) {
            indexOptions.unique(true);
        }
        if (options.containsKey("name")) {
            indexOptions.name(options.getString("name"));
        }
        return indexOptions;
    }

    @Override
    public String toString() {
        return toJs();
    }
}
