package io.bifroest.ymir.ymir.statistics;

import io.bifroest.commons.statistics.process.ProcessFinishedEvent;

public class YmirFinishedEvent extends ProcessFinishedEvent {

    public YmirFinishedEvent( long timestamp, boolean success ) {
        super( timestamp, success );
    }

}
