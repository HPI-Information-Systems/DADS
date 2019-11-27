package de.hpi.msc.jschneider.protocol.reaper;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class ReaperMessages
{
    @NoArgsConstructor @SuperBuilder
    public static class WatchMeMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 4300886325956393123L;
    }
}
