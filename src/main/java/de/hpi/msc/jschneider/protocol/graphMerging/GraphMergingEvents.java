package de.hpi.msc.jschneider.protocol.graphMerging;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

public class GraphMergingEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class GraphReceivedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -5337592448259457784L;
        @NonNull
        private Int2ObjectMap<GraphEdge> graph;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .graph(getGraph())
                            .build();
        }
    }
}
