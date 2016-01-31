package com.goodgame.profiling.bifroest_ymir.cassandra;

public interface EnvironmentWithMutableCassandra extends EnvironmentWithCassandra {

    void setCassandraAccessLayer( CassandraAccessLayer cassandra );

}
