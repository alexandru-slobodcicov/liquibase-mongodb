package liquibase.nosql.parser.json;

import liquibase.changelog.ChangeLogParameters;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class JsonNoSqlChangeLogParserTest {

    @SneakyThrows
    @Test
    void parse() {
        final ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();
        JsonNoSqlChangeLogParser parser = new JsonNoSqlChangeLogParser();
        parser.parse("liquibase/ext/json/changelog.generic.json", new ChangeLogParameters(), resourceAccessor);


    }

    @Test
    void supports() {
    }

    @Test
    void getSupportedFileExtensions() {
    }

    @Test
    void getPriority() {
    }

    @Test
    void getLogger() {
    }
}