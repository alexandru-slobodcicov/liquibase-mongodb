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
db.createCollection("databaseChangeLogLock", 
{
    validator: 
    {
        $jsonSchema: {
            bsonType: "object",
            description: "Database Lock Collection",
            required: ["_id", "locked"],
            properties: {
                _id: {
                    bsonType: "int",
                    description: "Unique lock identifier"
                },
                locked: {
                    bsonType: "bool",
                    description: "Lock flag"
                },
                lockGranted: {
                    bsonType: "date",
                    description: "Timestamp when lock acquired"
                },
                lockedBy: {
                    bsonType: "string",
                    description: "Owner of the lock"
                }
            }
        }
    },
    validationLevel: "strict",
    validationAction: "error", 
}
);
