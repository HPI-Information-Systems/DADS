package de.hpi.msc.jschneider.protocol.reaper;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class ReaperEvents
{
    @NoArgsConstructor @SuperBuilder
    public static class ActorSystemReapedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -1134340329548725936L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .build();
        }
    }
}
