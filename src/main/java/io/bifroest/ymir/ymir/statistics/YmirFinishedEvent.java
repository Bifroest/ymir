package com.goodgame.profiling.bifroest_ymir.ymir.statistics;

import com.goodgame.profiling.commons.statistics.process.ProcessFinishedEvent;

public class YmirFinishedEvent extends ProcessFinishedEvent {

    public YmirFinishedEvent( long timestamp, boolean success ) {
        super( timestamp, success );
    }

}
