package io.bifroest.ymir.ymir;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.commons.statistics.eventbus.EventBusManager;

import io.bifroest.ymir.ymir.statistics.YmirStartedEvent;
import io.bifroest.ymir.ymir.statistics.YmirFinishedEvent;
import io.bifroest.ymir.cassandra.EnvironmentWithCassandra;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;
import io.bifroest.retentions.RetentionTable;
import io.bifroest.retentions.RetentionLevel;

public class Ymir<E extends EnvironmentWithCassandra
                          & EnvironmentWithRetentionStrategy
                          & EnvironmentWithJSONConfiguration>
                    implements Runnable {

    private static final Logger log = LogManager.getLogger();

    private final E environment;
    private final Duration howLongToCreateTablesIntoTheFuture;

    public Ymir( E environment, Duration howLongToCreateTablesIntoTheFuture ) {
        this.environment = environment;
        this.howLongToCreateTablesIntoTheFuture = howLongToCreateTablesIntoTheFuture;
    }

    @Override
    public void run() {
        log.info( "Starting table preallocation" );

        try {
            EventBusManager.synchronousFire( new YmirStartedEvent( System.currentTimeMillis() ) );

            for( RetentionTable table : figureOutWhichTablesToCreate() ) {
                environment.cassandraAccessLayer().createTable( table );
            }

            EventBusManager.synchronousFire( new YmirFinishedEvent( System.currentTimeMillis(), true ) );
        } catch( Exception e ) {
            log.warn( "A totally unexpected exception occured", e );
            EventBusManager.synchronousFire( new YmirFinishedEvent( System.currentTimeMillis(), false ) );
        }
        log.info( "Finished table preallocation" );
    }

    private List<RetentionTable> figureOutWhichTablesToCreate() {
        log.debug( "Figure out which tables to create" );
        List<RetentionTable> result = new ArrayList<>();
        for( RetentionLevel level : environment.retentions().getAllLevels() ) {
            for( RetentionTable table : figureOutWhichTablesToCreateFor( level ) ) {
                result.add( table );
            }
        }
        log.debug( result );
        return result;
    }

    private List<RetentionTable> figureOutWhichTablesToCreateFor( RetentionLevel level ) {
        List<RetentionTable> result = new ArrayList<>();
        long now = System.currentTimeMillis() / 1000l;
        long blockNow = level.indexOf( now );
        long blockEnd = level.indexOf( now + howLongToCreateTablesIntoTheFuture.getSeconds() );
        for ( long idx = blockNow; idx <= blockEnd; idx ++ ) {
            result.add( new RetentionTable( level, idx ) );
        }
        return result;
    }
}
