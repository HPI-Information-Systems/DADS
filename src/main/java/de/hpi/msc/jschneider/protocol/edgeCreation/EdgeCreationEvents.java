package de.hpi.msc.jschneider.protocol.edgeCreation;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.time.LocalDateTime;

public class EdgeCreationEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class LocalGraphPartitionCreatedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -8606899178152425096L;
        @NonNull
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

    @NoArgsConstructor @SuperBuilder @Getter
    public static class EdgePartitionCreationCompletedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -4156964801073580719L;
        @NonNull
        private LocalDateTime startTime;
        @NonNull
        private LocalDateTime endTime;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .startTime(getStartTime())
                            .endTime(getEndTime())
                            .build();
        }
    }
}
