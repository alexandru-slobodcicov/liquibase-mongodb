package liquibase.ext.mongodb.executor;

import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.AbstractMongoIntegrationTest;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

class MongoExecutorIntegrationTest extends AbstractMongoIntegrationTest {

    @Test
    void testGetInstance() {
        final Executor executor = ExecutorService.getInstance().getExecutor(database);
        assertThat(executor, notNullValue());
        assertThat(executor, instanceOf(MongoExecutor.class));
    }

    @Test
    void setDatabase() {
    }

    @Test
    void queryForObject() {
    }

    @Test
    void queryForObject1() {
    }

    @Test
    void queryForLong() {
    }

    @Test
    void queryForLong1() {
    }

    @Test
    void queryForInt() {
    }

    @Test
    void queryForInt1() {
    }

    @Test
    void queryForList() {
    }

    @Test
    void queryForList1() {
    }

    @Test
    void queryForList2() {
    }

    @Test
    void queryForList3() {
    }

    @Test
    void execute() {
    }

    @Test
    void execute1() {
    }

    @Test
    void update() {
    }

    @Test
    void update1() {
    }

    @Test
    void comment() {
    }

    @Test
    void updatesDatabase() {
    }

    @Test
    void setDb() {
    }

    @Test
    void getDb() {
    }
}