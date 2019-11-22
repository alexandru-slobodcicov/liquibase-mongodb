package liquibase.ext.mongodb.statement;

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
