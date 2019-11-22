package liquibase.ext.mongodb.changelog;

import liquibase.change.CheckSum;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.bson.Document;

import static java.util.Objects.nonNull;
import static liquibase.util.StringUtils.isNotEmpty;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ChangeSetUtils {

    public static Document toDocument(RanChangeSet ranChangeSet) {

        final Document document = new Document()
            .append("id", ranChangeSet.getId())
            .append("author", ranChangeSet.getAuthor())
            .append("fileName", ranChangeSet.getChangeLog())
            .append("dateExecuted", ranChangeSet.getDateExecuted())
            .append("execType", ranChangeSet.getExecType().value)
            .append("md5sum", ranChangeSet.getLastCheckSum().toString())
            .append("description", ranChangeSet.getDescription());

        if (nonNull(ranChangeSet.getOrderExecuted())) {
            document.append("orderExecuted", ranChangeSet.getOrderExecuted());
        }

        if (isNotEmpty(ranChangeSet.getComments())) {
            document.append("comments", ranChangeSet.getComments());
        }

        if (isNotEmpty(ranChangeSet.getTag())) {
            document.append("tag", ranChangeSet.getTag());
        }

        if (isNotEmpty(ranChangeSet.getDeploymentId())) {
            document.append("liquibase", ranChangeSet.getDeploymentId());
        }

        return document;
    }

    public static RanChangeSet fromDocument(Document document) {

        final RanChangeSet ranChangeSet = new RanChangeSet(
            document.getString("fileName"),
            document.getString("id"),
            document.getString("author"),
            CheckSum.parse(document.getString("md5sum")),
            document.getDate("dateExecuted"),
            document.getString("tag"),
            ChangeSet.ExecType.valueOf(document.getString("execType")),
            document.getString("description"),
            document.getString("comments"),
            null,
            null,
            document.getString("liquibase")
        );
        ranChangeSet.setOrderExecuted(document.getInteger("orderExecuted"));

        return ranChangeSet;
    }

}
