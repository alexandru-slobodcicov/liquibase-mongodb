package liquibase.ext.mongodb.configuration;

import liquibase.configuration.AbstractConfigurationContainer;

import static java.lang.Boolean.TRUE;

public class MongoConfiguration extends AbstractConfigurationContainer {


    public static final String LIQUIBASE_MONGO_NAMESPACE = "liquibase.mongodb";
    public static final String ADJUST_TRACKING_TABLES_ON_STARTUP = "adjustTrackingTablesOnStartup";
    public static final String SUPPORTS_VALIDATOR = "supportsValidator";

    public MongoConfiguration() {
        this(LIQUIBASE_MONGO_NAMESPACE);
    }

    /**
     * Subclasses must call this constructor passing the namespace, but must themselves provide a no-arg public constructor.
     *
     * @param namespace - default property prefix
     */
    protected MongoConfiguration(final String namespace) {

        super(namespace);

        getContainer().addProperty(ADJUST_TRACKING_TABLES_ON_STARTUP, Boolean.class)
                .setDescription("Enabling this property will validate History Change Log and Log Lock Collections " +
                        "on Startup and adjust if are not up to date with current release." +
                        "Worth keeping it disabled and re-enable when upgraded to a new Liquibase version.")
                .setDefaultValue(TRUE);

        getContainer().addProperty(SUPPORTS_VALIDATOR, Boolean.class)
                .setDescription("Disabling this property will let create the Tracking Collections without validators." +
                        "This will permit usage on Mongo Versions not supporting Validators")
                .setDefaultValue(TRUE);
    }

    /**
     * Adjust DATABASECHANGELOG and DATABASECHANGELOGLOCK on Startup
     */
    public Boolean getAdjustTrackingTablesOnStartup() {
        return getContainer().getValue(ADJUST_TRACKING_TABLES_ON_STARTUP, Boolean.class);
    }

    public MongoConfiguration setAdjustTrackingTablesOnStartup(final Boolean value) {
        getContainer().setValue(ADJUST_TRACKING_TABLES_ON_STARTUP, value);
        return this;
    }

    /**
     * Add Validator to DATABASECHANGELOG and DATABASECHANGELOGLOCK on Creation and Startup Adjustment
     */
    public Boolean getSupportsValidator() {
        return getContainer().getValue(SUPPORTS_VALIDATOR, Boolean.class);
    }

    public MongoConfiguration setSupportsValidator(final Boolean value) {
        getContainer().setValue(SUPPORTS_VALIDATOR, value);
        return this;
    }
}
