package liquibase.ext.mongodb.change;

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

import liquibase.ChecksumVersion;
import liquibase.GlobalConfiguration;
import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.change.AbstractSQLChange;
import liquibase.change.CheckSum;
import liquibase.database.Database;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.util.StringUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public abstract class AbstractMongoChange extends AbstractChange {

    @Override
    public boolean supports(Database database) {
        return database instanceof MongoLiquibaseDatabase;
    }

    @Override
    public boolean generateStatementsVolatile(final Database database) {
        return false;
    }

    @Override
    public boolean generateRollbackStatementsVolatile(final Database database) {
        return false;
    }

    /**
     * Generate checksum normalizing texts so changes as adding white spaces and new lines
     * won't break the checksum
     */
    CheckSum generateCheckSum(String... texts) {
        ChecksumVersion version = Scope.getCurrentScope().getChecksumVersion();
        if (version.lowerOrEqualThan(ChecksumVersion.V8)) {
            return super.generateCheckSum();
        }

        String text = "";
        if (texts != null) {
            text = StringUtil.join(texts, "");
        }


        try (InputStream stream = new ByteArrayInputStream(text.getBytes(GlobalConfiguration.FILE_ENCODING.getCurrentValue()))) {
            return CheckSum.compute(new AbstractSQLChange.NormalizingStream(stream), false);
        } catch (IOException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }
}
