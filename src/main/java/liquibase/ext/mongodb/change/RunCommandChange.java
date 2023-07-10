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

import liquibase.change.ChangeMetaData;
import liquibase.change.CheckSum;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.mongodb.statement.RunCommandStatement;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@DatabaseChange(name = "runCommand",
    description = "Provides a helper to run specified database commands. This is the preferred method to issue database commands, as it provides a consistent interface between the shell and drivers. " +
        "https://docs.mongodb.com/manual/reference/method/db.runCommand/#db-runcommand",
    priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "database")
@NoArgsConstructor
@Getter
@Setter
public class RunCommandChange extends AbstractMongoChange {

    private String command;

    @Override
    public String getConfirmationMessage() {
        return "Command run";
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[]{
            new RunCommandStatement(command)
        };
    }

    @Override
    public CheckSum generateCheckSum() {
        return super.generateCheckSum(command);
    }
}
