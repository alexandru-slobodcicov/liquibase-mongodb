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