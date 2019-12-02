package de.hpi.msc.jschneider.protocol.reaper;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class ReaperEvents
{
    @NoArgsConstructor @SuperBuilder
    public static class ActorSystemReapedEvents extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -1134340329548725936L;
    }
}
