package com.goodgame.profiling.bifroest_ymir.cassandra;

import com.goodgame.profiling.commons.boot.interfaces.Environment;

public interface EnvironmentWithCassandra extends Environment {

    CassandraAccessLayer cassandraAccessLayer();

}
