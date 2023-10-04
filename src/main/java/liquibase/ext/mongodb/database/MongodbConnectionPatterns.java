package liquibase.ext.mongodb.database;

import liquibase.database.jvm.ConnectionPatterns;

import java.util.regex.Pattern;


public class MongodbConnectionPatterns extends ConnectionPatterns {
    public static final String FILTER_CREDS_MONGODB_TO_OBFUSCATE = "(?i).+://(.*?)([:])(.*?)((?=@))";

    public MongodbConnectionPatterns() {
        addJdbcBlankToObfuscatePatterns(PatternPair.of(Pattern.compile("(?i)mongodb(.*)"), Pattern.compile(FILTER_CREDS_MONGODB_TO_OBFUSCATE)));
        addJdbcBlankToObfuscatePatterns(PatternPair.of(Pattern.compile("(?i)mongodb+srv(.*)"), Pattern.compile(FILTER_CREDS_MONGODB_TO_OBFUSCATE)));
    }
}
