package io.bifroest.ymir.ymir;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.SystemIdentifiers;
import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.commons.cron.TaskRunner;
import io.bifroest.commons.cron.TaskRunner.TaskID;
import io.bifroest.commons.statistics.units.parse.DurationParser;

import io.bifroest.ymir.YmirIdentifiers;
import io.bifroest.ymir.cassandra.EnvironmentWithCassandra;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;

@MetaInfServices
public class YmirSystem<E extends EnvironmentWithJSONConfiguration
                                & EnvironmentWithRetentionStrategy
                                & EnvironmentWithCassandra>
                          implements Subsystem<E> {

    private static final Logger log = LogManager.getLogger();
    private static final DurationParser parser = new DurationParser( );

    private Ymir<E> ymir;
    private TaskID task;

    @Override
    public String getSystemIdentifier() {
        return YmirIdentifiers.YMIR;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( SystemIdentifiers.STATISTICS, SystemIdentifiers.RETENTION, YmirIdentifiers.CASSANDRA );
    }

    @Override
    public void configure( JSONObject configuration ) {
        // empty
    }

    @Override
    public void boot( final E environment ) {
        JSONObject config = environment.getConfiguration().getJSONObject( "ymir" );
        Duration frequency = parser.parse( config.getString( "run-every" ) );
        Duration duration = parser.parse( config.getString( "create-tables-for" ) );

        ymir = new Ymir<E>( environment, duration );
        task = TaskRunner.runRepeated( ymir, "Ymir", Duration.ZERO, frequency, false );
    }

    @Override
    public void shutdown( E environment ) {
        TaskRunner.stopTask( task );
    }
}
