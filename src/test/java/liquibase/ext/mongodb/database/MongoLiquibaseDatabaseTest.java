package liquibase.ext.mongodb.database;

import liquibase.configuration.LiquibaseConfiguration;
import liquibase.database.DatabaseFactory;
import liquibase.ext.mongodb.configuration.MongoConfiguration;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;

import static java.lang.Boolean.FALSE;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class MongoLiquibaseDatabaseTest {

    protected MongoLiquibaseDatabase database;
    protected MongoConfiguration configuration;

    @SneakyThrows
    @BeforeEach
    void setUp() {
        resetServices();
        database = (MongoLiquibaseDatabase) DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(new MongoConnection());
        configuration = LiquibaseConfiguration.getInstance()
                .getConfiguration(MongoConfiguration.class);
    }

    @AfterEach
    void tearDown() {
        resetServices();
    }

    protected void resetServices() {
        DatabaseFactory.reset();
        LiquibaseConfiguration.getInstance().reset();
    }

    @Test
    void getDefaultDriver() {
        assertThat(database.getDefaultDriver("mongodb://qwe")).isEqualTo(MongoClientDriver.class.getName());
        assertThat(database.getDefaultDriver("cosmos://qwe")).isNull();

    }

    @Test
    void getDatabaseProductName() {
        assertThat(database.getDatabaseProductName()).isEqualTo("MongoDB");
    }

    @Test
    void getShortName() {
        assertThat(database.getShortName()).isEqualTo("mongodb");
    }

    @Test
    void getDefaultPort() {
        assertThat(database.getDefaultPort()).isEqualTo(27017);
    }

    @Test
    void getDefaultDatabaseProductName() {
        assertThat(database.getDefaultDatabaseProductName()).isEqualTo("MongoDB");
    }

    @Test
    void getAdjustTrackingTablesOnStartup() {
        assertThat(configuration.getAdjustTrackingTablesOnStartup()).isTrue();
        assertThat(database.getAdjustTrackingTablesOnStartup()).isTrue();
        configuration.setAdjustTrackingTablesOnStartup(FALSE);
        assertThat(configuration.getAdjustTrackingTablesOnStartup()).isFalse();
        assertThat(database.getAdjustTrackingTablesOnStartup()).isFalse();
    }

    @Test
    void getSupportsValidator() {
        assertThat(configuration.getSupportsValidator()).isTrue();
        assertThat(database.getSupportsValidator()).isTrue();
        configuration.setSupportsValidator(FALSE);
        assertThat(configuration.getSupportsValidator()).isFalse();
        assertThat(database.getSupportsValidator()).isFalse();
    }

    @Test
    void setAdjustTrackingTablesOnStartup() {
        assertThat(configuration.getAdjustTrackingTablesOnStartup()).isTrue();
        assertThat(database.getAdjustTrackingTablesOnStartup()).isTrue();
        database.setAdjustTrackingTablesOnStartup(FALSE);
        assertThat(configuration.getAdjustTrackingTablesOnStartup()).isTrue();
        assertThat(database.getAdjustTrackingTablesOnStartup()).isFalse();
    }

    @Test
    void setSupportsValidator() {
        assertThat(configuration.getSupportsValidator()).isTrue();
        assertThat(database.getSupportsValidator()).isTrue();
        database.setSupportsValidator(FALSE);
        assertThat(configuration.getSupportsValidator()).isTrue();
        assertThat(database.getSupportsValidator()).isFalse();
    }
}