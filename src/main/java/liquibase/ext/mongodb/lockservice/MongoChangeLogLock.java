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

import liquibase.lockservice.DatabaseChangeLogLock;
import org.bson.Document;

import java.util.Date;

public class MongoChangeLogLock extends DatabaseChangeLogLock{

    private int id;
    private Date lockGranted;
    private String lockedBy;
    private Boolean locked;

    public MongoChangeLogLock() {
        this(1, new Date(), "NoArgConstructor", true);
    }

    public MongoChangeLogLock(final Integer id, final Date lockGranted, final String lockedBy, final Boolean locked) {
        super(id, lockGranted, lockedBy);
        this.id = id;
        this.lockGranted = lockGranted;
        this.lockedBy = lockedBy;
        this.locked = locked;
    }

    //TODO: use  db.getCollection(collectionName, requiredType).withCodecRegistry(MongoConnection.pojoCodecRegistry())
    //not working when converting back to POJO, date field is as object, String is as binary
    public static MongoChangeLogLock from(final Document document) {

        return new MongoChangeLogLock(
                document.get("_id", Integer.class)
                , document.get("lockGranted", Date.class)
                , document.get("lockedBy", String.class)
                , document.get("locked", Boolean.class)
        );
    }

    @Override
    public int getId() {
        return id;
    }

    public void setId(final int id) {
        this.id = id;
    }

    @Override
    public Date getLockGranted() {
        return lockGranted;
    }

    public void setLockGranted(final Date lockGranted) {
        this.lockGranted = lockGranted;
    }

    @Override
    public String getLockedBy() {
        return lockedBy;
    }

    public void setLockedBy(final String lockedBy) {
        this.lockedBy = lockedBy;
    }

    public Boolean getLocked() {
        return locked;
    }

    public void setLocked(final Boolean locked) {
        this.locked = locked;
    }

    @Override
    public String toString() {
        return toDocument().toJson();
    }

    public Document toDocument() {
        return new Document()
                .append("_id", id)
                .append("lockGranted", lockGranted)
                .append("lockedBy", lockedBy)
                .append("locked", locked);
    }

}
