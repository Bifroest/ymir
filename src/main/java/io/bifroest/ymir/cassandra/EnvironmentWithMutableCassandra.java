package io.bifroest.ymir.cassandra;

public interface EnvironmentWithMutableCassandra extends EnvironmentWithCassandra {

    void setCassandraAccessLayer( CassandraAccessLayer cassandra );

}
