package de.hpi.msc.jschneider.actor.common.reaper;

import de.hpi.msc.jschneider.actor.common.AbstractMessage;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

public class ReaperMessages
{
    @SuperBuilder @Getter
    public static class WatchMeMessage extends AbstractMessage
    {
        private static final long serialVersionUID = 8417465868705037933L;
    }
}
