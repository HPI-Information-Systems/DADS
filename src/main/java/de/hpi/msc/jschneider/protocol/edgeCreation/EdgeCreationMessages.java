package de.hpi.msc.jschneider.protocol.edgeCreation;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class EdgeCreationMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class LastNodeMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -4416907586342634262L;
        private GraphNode lastNode;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .lastNode(getLastNode())
                            .build();
        }
    }
}
