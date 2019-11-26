package liquibase.ext.mongodb;

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

import liquibase.Liquibase;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.LiquibaseException;
import liquibase.ext.mongodb.database.MongoConnection;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.util.file.FilenameUtils;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static lombok.AccessLevel.PRIVATE;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class TestUtils {
    public static final String BUILD_INFO_1 = "{ buildInfo: 1 }";
    public static final String COLLECTION_NAME_1 = "collectionName";
    public static final String EMPTY_STRING = "";
    public static final String EMPTY_OPTION = EMPTY_STRING;
    public static final String OPTION_1 = "{\"opt1\":\"option 1\"}";
    private static final String DB_CONNECTION_PATH = "db.connection.uri";
    private static final String CMD_DROP_ALL_ROLES = "{dropAllRolesFromDatabase: 1, writeConcern: { w: \"majority\" }}";
    private static final String CMD_DROP_ALL_USERS = "{dropAllUsersFromDatabase: 1, writeConcern: { w: \"majority\" }}";
    private static final String CMD_GET_ALL_ROLES = "{getAllRolesFromDatabase: 1,  showPrivileges:false, showBuiltinRoles: false }}";

    public static List<String> getCollections(final MongoConnection connection) {
        return StreamSupport.stream(connection.getDb().listCollectionNames().spliterator(), false)
            .collect(Collectors.toList());
    }

    @SneakyThrows
    public static MongoConnection getMongoConnection(final String propertyFile) {
        final Properties properties = new Properties();
        properties.load(TestUtils.class.getClassLoader().getResourceAsStream(propertyFile));
        log.debug("Actual connection properties:\n{}", properties);

        return getMongoConnection(properties);
    }

    public static MongoConnection getMongoConnection(final Properties properties) {
        final String connectionUri = properties.getProperty(DB_CONNECTION_PATH);
        assertThat(connectionUri).isNotBlank();
        return new MongoConnection(connectionUri);
    }

    public static void dropAllRoles(final MongoConnection connection) {
        connection.getDb().runCommand(Document.parse(CMD_DROP_ALL_ROLES));
    }

    public static List<Document> getAllRoles(final MongoConnection connection) {
        return (List<Document>) connection.getDb()
            .runCommand(Document.parse("{ rolesInfo: 1, showPrivileges:false, showBuiltinRoles: false }"))
            .get("roles");
    }

    public static void dropAllUsers(final MongoConnection connection) {
        connection.getDb().runCommand(Document.parse(CMD_DROP_ALL_USERS));
    }

    public static void commit(final Liquibase liquibase) throws LiquibaseException {
        liquibase.update("{}");
    }

    public static List<ChangeSet> getChangesets(final String changeSetPath, final MongoLiquibaseDatabase database) throws LiquibaseException {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        final ChangeLogParser parser =
            ChangeLogParserFactory.getInstance().getParser(FilenameUtils.getExtension(changeSetPath), resourceAccessor);

        final DatabaseChangeLog changeLog =
            parser.parse(changeSetPath, new ChangeLogParameters(database), resourceAccessor);
        return changeLog.getChangeSets();
    }
}
