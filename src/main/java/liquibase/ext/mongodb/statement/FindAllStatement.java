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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static liquibase.ext.mongodb.statement.BsonUtils.orEmptyDocument;

@Getter
@EqualsAndHashCode(callSuper = true)
public class FindAllStatement extends AbstractMongoStatement {

    public static final String COMMAND = "find";

    private final String collectionName;
    private final Document filter;
    private final Document sort;

    public FindAllStatement(final String collectionName) {
        this(collectionName, new Document(), new Document());
    }

    public FindAllStatement(final String collectionName, final String filter, final String sort) {
        this(collectionName, orEmptyDocument(filter), orEmptyDocument(sort));
    }

    public FindAllStatement(final String collectionName, final Document filter, final Document sort) {
        this.collectionName = collectionName;
        this.filter = filter;
        this.sort = sort;
    }

    @Override
    public String toJs() {
        return
            "db." +
                collectionName +
                "." +
                COMMAND +
                "(" +
                filter.toJson() +
                ");";
    }

    @Override
    public List queryForList(final MongoDatabase db, final Class elementType) throws DatabaseException {
        final ArrayList result = new ArrayList();
        db.getCollection(collectionName, elementType)
            .find(filter).sort(sort).into(result);
        return result;
    }

    @Override
    public String toString() {
        return toJs();
    }
}
