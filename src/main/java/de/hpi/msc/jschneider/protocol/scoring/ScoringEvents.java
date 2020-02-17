package de.hpi.msc.jschneider.protocol.scoring;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class ScoringEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ReadyForTerminationEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -2548952160769122196L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .build();
        }
    }
}
