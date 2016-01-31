package io.bifroest.ymir;

import java.nio.file.Path;

import io.bifroest.commons.boot.InitD;
import io.bifroest.commons.environment.AbstractCommonEnvironment;
import io.bifroest.ymir.cassandra.CassandraAccessLayer;
import io.bifroest.ymir.cassandra.EnvironmentWithMutableCassandra;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.bootloader.EnvironmentWithMutableRetentionStrategy;

public class YmirEnvironment extends AbstractCommonEnvironment
    implements EnvironmentWithMutableCassandra,
               EnvironmentWithMutableRetentionStrategy {

    private RetentionConfiguration retention;
    private CassandraAccessLayer cassandra;

    public YmirEnvironment( Path configPath, InitD init ) {
        super( configPath, init );
    }
    @Override
    public RetentionConfiguration retentions() {
        return retention;
    }

    @Override
    public void setRetentions( RetentionConfiguration retention ) {
        this.retention = retention;
    }

    @Override
    public CassandraAccessLayer cassandraAccessLayer() {
        return cassandra;
    }

    @Override
    public void setCassandraAccessLayer( CassandraAccessLayer cassandra ) {
        this.cassandra = cassandra;
    }
}
