package io.bifroest.ymir.cassandra;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TableMetadata;

import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.ymir.cassandra.statistics.CreateTableEvent;
import io.bifroest.retentions.RetentionTable;

public class CassandraAccessLayer {

    private static final Logger log = LogManager.getLogger();

    private static final String COL_NAME = "metric";
    private static final String COL_TIME = "timestamp";
    private static final String COL_VALUE = "value";

    private final String user;
    private final String pass;
    private final String keyspace;
    private final String[] hosts;
    private final boolean dryRun;
    private final Duration readTimeout;

    private Cluster cluster;
    private Session session = null;

    public CassandraAccessLayer( String user, String pass, String keyspace, String[] hosts, boolean dryRun, Duration readTimeout ){
        this.user = user;
        this.pass = pass;
        this.keyspace = keyspace;
        this.hosts = hosts;
        this.dryRun = dryRun;
        this.readTimeout = readTimeout;

        if ( dryRun ) {
            log.warn( "Running with dryRun, NOT ACTUALLY DOING ANYTHING!!!" );
        }
    }

    public void open() {
        log.info( "Open Connection with Cassandra" );
        if ( cluster == null || session == null ) {
            Builder builder = Cluster.builder();
            builder.addContactPoints( hosts );
            builder.withSocketOptions( ( new SocketOptions().setReadTimeoutMillis( (int)readTimeout.toMillis() ) ) );
            if ( user != null && pass != null && !user.isEmpty() && !pass.isEmpty() ) {
                builder = builder.withCredentials( user, pass );
            }
            cluster = builder.build();
            session = cluster.connect( keyspace );
        }
    }

    public void close() {
        log.info( "Close Connection with Cassandra" );
        if ( session != null ) {
            session.close();
            session = null;
        }
        if ( cluster != null ) {
            cluster.close();
            cluster = null;
        }
    }

    public void createTable( RetentionTable table ) {
        log.info( "Creating table " + table );
        if ( session == null ) {
            open();
        }

        if ( dryRun ) {
            log.debug( "Dry Run" );
            return;
        }

        StringBuilder query = new StringBuilder();
        query.append( "CREATE TABLE IF NOT EXISTS " ).append( table.tableName() ).append( " (" );
        query.append( COL_NAME ).append( " text, " );
        query.append( COL_TIME ).append( " bigint, " );
        query.append( COL_VALUE ).append( " double, " );
        query.append( "PRIMARY KEY (" ).append( COL_NAME ).append( ", " ).append( COL_TIME ).append( ")" );
        query.append( ");" );
        session.execute( query.toString() );
        EventBusManager.fire( new CreateTableEvent( System.currentTimeMillis(), table ) );
    }
}
