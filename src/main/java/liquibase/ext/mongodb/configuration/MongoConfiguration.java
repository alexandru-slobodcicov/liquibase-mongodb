package liquibase.ext.mongodb.configuration;

import liquibase.configuration.AutoloadedConfigurations;
import liquibase.configuration.ConfigurationDefinition;

import static java.lang.Boolean.TRUE;

public class MongoConfiguration implements AutoloadedConfigurations {


    public static final String LIQUIBASE_MONGO_NAMESPACE = "liquibase.mongodb";
    public static final ConfigurationDefinition<Boolean>  ADJUST_TRACKING_TABLES_ON_STARTUP;
    public static final ConfigurationDefinition<Boolean>  SUPPORTS_VALIDATOR;
    static {
        liquibase.configuration.ConfigurationDefinition.Builder builder = new ConfigurationDefinition.Builder(LIQUIBASE_MONGO_NAMESPACE);


        ADJUST_TRACKING_TABLES_ON_STARTUP = builder.define("adjustTrackingTablesOnStartup", Boolean.class)
                                .setDescription("Enabling this property will validate History Change Log and Log Lock Collections " +
                "on Startup and adjust if are not up to date with current release." +
                "Worth keeping it disabled and re-enable when upgraded to a new Liquibase version.")
                .setDefaultValue(TRUE)
        .build();

        SUPPORTS_VALIDATOR = builder.define("supportsValidator", Boolean.class)
                .setDescription("Disabling this property will let create the Tracking Collections without validators." +
                        "This will permit usage on Mongo Versions not supporting Validators")
                .setDefaultValue(TRUE)
                .build();
    }
}
