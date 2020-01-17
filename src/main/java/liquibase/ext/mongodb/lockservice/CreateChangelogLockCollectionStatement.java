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
