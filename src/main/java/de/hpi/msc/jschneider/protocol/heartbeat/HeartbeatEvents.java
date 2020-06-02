package de.hpi.msc.jschneider.protocol.heartbeat;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class HeartbeatEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class StillAliveEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 5017458197615182392L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class HeartbeatPanicEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 9103052774901729016L;

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
