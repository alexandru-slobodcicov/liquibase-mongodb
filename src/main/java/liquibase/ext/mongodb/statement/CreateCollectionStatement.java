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
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.client.model.ValidationOptions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.bson.Document;

import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;

@Getter
@EqualsAndHashCode(callSuper = true)
public class CreateCollectionStatement extends AbstractMongoStatement {

    public static final String COMMAND = "createCollection";

    protected String collectionName;
    protected Document options;

    public CreateCollectionStatement(String collectionName, String options) {
        this(collectionName, BsonUtils.orEmptyDocument(options));
    }

    public CreateCollectionStatement(final String collectionName, final Document options) {
        this.collectionName = collectionName;
        this.options = options;
    }

    @Override
    public String toJs() {
        return
                "db."
                        + COMMAND
                        + "("
                        + collectionName
                        + ", "
                        + options.toJson()
                        + ");";
    }

    @Override
    public void execute(MongoDatabase db) {
        final CreateCollectionOptions createCollectionOptions =
                new CreateCollectionOptions();

        if (nonNull(options)) {
            final ValidationAction
                    validationAction =
                    ofNullable(this.options.getString("validationAction"))
                            .map(ValidationAction::fromString)
                            .orElse(null);

            final ValidationLevel
                    validationLevel =
                    ofNullable(this.options.getString("validationLevel"))
                            .map(ValidationLevel::fromString)
                            .orElse(null);

            createCollectionOptions.validationOptions(
                    new ValidationOptions()
                            .validationAction(validationAction)
                            .validationLevel(validationLevel)
                            .validator(this.options.get("validator", Document.class)));
        }

        db.createCollection(collectionName, createCollectionOptions);
    }

    @Override
    public String toString() {
        return toJs();
    }
}
