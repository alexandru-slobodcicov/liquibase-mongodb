package liquibase.ext.mongodb.changelog;

import liquibase.ContextExpression;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Labels;
import liquibase.change.CheckSum;
import liquibase.changelog.ChangeSet;
import liquibase.ext.mongodb.statement.InsertOneStatement;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MongoRanChangeSetToDocumentConverterTest {

    protected MongoRanChangeSetToDocumentConverter converter = new MongoRanChangeSetToDocumentConverter();

    @Test
    void toDocument() {
    }

    @Test
    void fromDocument() {

        // Maximum
        final Date dateExecuted = new Date();
        final Document maximal = new Document()
                .append("id", "cs4")
                .append("author", "Alex")
                .append("fileName", "liquibase/file.xml")
                .append("dateExecuted", dateExecuted)
                .append("orderExecuted", 100)
                .append("execType", "EXECUTED")
                .append("md5sum", "8:c3981fa8d26e95d911fe8eaeb6570f2f")
                .append("description", "The Description")
                .append("comments", "The Comments")
                .append("tag", "Tags")
                .append("contexts", "context1,context2")
                .append("labels", "label1,label2")
                .append("deploymentId", "The Deployment Id")
                .append("liquibase", "Liquibase Version");

        assertThat(converter.fromDocument(maximal))
                .isInstanceOf(MongoRanChangeSet.class)
                .returns("cs4", MongoRanChangeSet::getId)
                .returns("Alex", MongoRanChangeSet::getAuthor)
                .returns("liquibase/file.xml", MongoRanChangeSet::getChangeLog)
                .returns(dateExecuted, MongoRanChangeSet::getDateExecuted)
                .returns(100, MongoRanChangeSet::getOrderExecuted)
                .returns(ChangeSet.ExecType.EXECUTED, MongoRanChangeSet::getExecType)
                .returns(CheckSum.compute("QWERTY"), MongoRanChangeSet::getLastCheckSum)
                .returns( "The Description", MongoRanChangeSet::getDescription)
                .returns( "The Comments", MongoRanChangeSet::getComments)
                .returns("Tags", MongoRanChangeSet::getTag)
                .returns(true, c-> c.getContextExpression().getContexts().containsAll(Arrays.asList("context1","context2")))
                .returns(true, c-> c.getLabels().getLabels().containsAll(Arrays.asList("label1", "label2")))
                .returns("The Deployment Id", MongoRanChangeSet::getDeploymentId)
                .returns("Liquibase Version", MongoRanChangeSet::getLiquibaseVersion);

        // Empty Document
        assertThat(converter.fromDocument(new Document()))
                .isInstanceOf(MongoRanChangeSet.class)
                .hasAllNullFieldsOrPropertiesExcept("contextExpression", "labels");
    }
}