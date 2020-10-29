package liquibase.nosql.changelog;

import liquibase.ContextExpression;
import liquibase.Labels;

import java.util.Collection;

import static liquibase.sqlgenerator.core.MarkChangeSetRanGenerator.*;

public abstract class AbstractNoSqlItemToDocumentConverter<I, D>  {

    public abstract D toDocument(I item);

    public abstract I fromDocument(D document);

    public String buildLabels(Labels labels) {
        if (labels == null || labels.isEmpty()) {
            return null;
        }
        return labels.toString();
    }

    public String buildFullContext(final ContextExpression contextExpression, final Collection<ContextExpression> inheritableContexts) {
        if ((contextExpression == null) || contextExpression.isEmpty()) {
            return null;
        }

        StringBuilder contextExpressionString = new StringBuilder();
        boolean notFirstContext = false;
        for (ContextExpression inheritableContext : inheritableContexts) {
            appendContext(contextExpressionString, inheritableContext.toString(), notFirstContext);
            notFirstContext = true;
        }
        appendContext(contextExpressionString, contextExpression.toString(), notFirstContext);

        return contextExpressionString.toString();
    }

    private void appendContext(StringBuilder contextExpression, String contextToAppend, boolean notFirstContext) {
        boolean complexExpression = contextToAppend.contains(COMMA) || contextToAppend.contains(WHITESPACE);
        if (notFirstContext) {
            contextExpression.append(AND);
        }
        if (complexExpression) {
            contextExpression.append(OPEN_BRACKET);
        }
        contextExpression.append(contextToAppend);
        if (complexExpression) {
            contextExpression.append(CLOSE_BRACKET);
        }
    }
}
