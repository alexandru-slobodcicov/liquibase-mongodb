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
db.createCollection("databaseChangeLog", 
{
    validator: {
        $jsonSchema: 
            {
            bsonType: "object",
            description: "Database Change Log Table.",
            required: ["_id", "id", "author", "fileName"],
            properties: {
                _id: {
                    bsonType: "objectId",
                    description: "Unique internal identifier."
                },
                id: {
                    bsonType: "string",
                    description: "Value from the changeSet id attribute."
                },
                author: {
                    bsonType: "string",
                    description: "Value from the changeSet author attribute."
                },
                fileName: {
                    bsonType: "string",
                    description: "Path to the changelog. This may be an absolute path or a relative path depending on how the changelog was passed to Liquibase. For best results, it should be a relative path."
                },
                dateExecuted: {
                    bsonType: "date",
                    description: "Date/time of when the changeSet was executed. Used with orderExecuted to determine rollback order."
                },
                orderExecuted: {
                    bsonType: "int",
                    description: "Order that the changeSets were executed. Used in addition to dateExecuted to ensure order is correct even when the databases datetime supports poor resolution."
                },
                execType: {
                    bsonType: "string",
                    enum: ["EXECUTED", "FAILED", "SKIPPED", "RERAN", "MARK_RAN"],
                    description: "Description of how the changeSet was executed."
                },
                md5sum: {
                    bsonType: "string",
                    description: "Checksum of the changeSet when it was executed. Used on each run to ensure there have been no unexpected changes to changSet in the changelog file."
                },
                description: {
                    bsonType: "string",
                    description: "Short auto-generated human readable description of changeSet."
                },
                comments: {
                    bsonType: "string",
                    description: "Value from the changeSet comment attribute."
                },
                tag: {
                    bsonType: "string",
                    description: "Tracks which changeSets correspond to tag operations."
                },
                liquibase: {
                    bsonType: "string",
                    description: "Version of Liquibase used to execute the changeSet."
                }
            }
        }
    },
    validationLevel: "strict",
    validationAction: "error", 
});
