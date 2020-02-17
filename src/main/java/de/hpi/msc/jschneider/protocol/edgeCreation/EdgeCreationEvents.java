package de.hpi.msc.jschneider.protocol.edgeCreation;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class EdgeCreationEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class LocalGraphPartitionCreatedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -8606899178152425096L;
        private Graph graphPartition;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .graphPartition(getGraphPartition())
                            .build();
        }
    }
}
