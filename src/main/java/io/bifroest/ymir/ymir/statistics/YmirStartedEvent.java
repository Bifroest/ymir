package io.bifroest.ymir.ymir.statistics;

import io.bifroest.commons.statistics.process.ProcessStartedEvent;

public class YmirStartedEvent extends ProcessStartedEvent {

    public YmirStartedEvent( long timestamp ) {
        super( timestamp );
    }

}
