package com.goodgame.profiling.bifroest_ymir.ymir;

import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.goodgame.profiling.commons.systems.configuration.EnvironmentWithJSONConfiguration;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;

import com.goodgame.profiling.bifroest_ymir.ymir.statistics.YmirStartedEvent;
import com.goodgame.profiling.bifroest_ymir.ymir.statistics.YmirFinishedEvent;
import com.goodgame.profiling.bifroest_ymir.cassandra.EnvironmentWithCassandra;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;
import com.goodgame.profiling.graphite_retentions.RetentionTable;
import com.goodgame.profiling.graphite_retentions.RetentionLevel;

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
