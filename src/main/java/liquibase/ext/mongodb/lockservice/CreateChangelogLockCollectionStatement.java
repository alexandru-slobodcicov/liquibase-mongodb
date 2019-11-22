package liquibase.ext.mongodb.lockservice;

import liquibase.ext.mongodb.statement.CreateCollectionStatement;

public class CreateChangelogLockCollectionStatement extends CreateCollectionStatement {

    private static final String VALIDATOR = "{\n"
        + "validator: {\n"
        + "     $jsonSchema: {\n"
        + "         bsonType: \"object\",\n"
        + "         description: \"Database Lock Collection\",\n"
        + "         required: [\"_id\", \"locked\"],\n"
        + "             properties: {\n"
        + "                 _id: {\n"
        + "                     bsonType: \"int\",\n"
        + "                     description: \"Unique lock identifier\"\n"
        + "                 },\n"
        + "                 locked: {\n"
        + "                     bsonType: \"bool\",\n"
        + "                     description: \"Lock flag\"\n"
        + "                 },\n"
        + "                 lockGranted: {\n"
        + "                     bsonType: \"date\",\n"
        + "                     description: \"Timestamp when lock acquired\"\n"
        + "                 },\n"
        + "                 lockedBy: {\n"
        + "                     bsonType: \"string\",\n"
        + "                     description: \"Owner of the lock\"\n"
        + "                 }\n"
        + "             }\n"
        + "         }\n"
        + "     },\n"
        + "validationAction: \"error\",\n"
        + "validationLevel: \"strict\"\n"
        + "}";

    public CreateChangelogLockCollectionStatement(final String collectionName) {
        super(collectionName, VALIDATOR);
    }
}