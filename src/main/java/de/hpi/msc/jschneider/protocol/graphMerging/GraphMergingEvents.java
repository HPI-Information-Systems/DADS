package de.hpi.msc.jschneider.protocol.graphMerging;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Map;

public class GraphMergingEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class GraphReceivedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -5337592448259457784L;
        private Map<Integer, GraphEdge> graph;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .graph(getGraph())
                            .build();
        }
    }
}
