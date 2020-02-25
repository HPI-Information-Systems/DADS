package de.hpi.msc.jschneider.protocol.nodeCreation.worker.nodeExtractor;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Set;

public class NodeExtractorMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class CreateNodeCollectionMessage extends ActorPoolMessages.WorkMessage
    {
        private static final long serialVersionUID = 6747070979140830498L;
        private int intersectionSegment;
        @NonNull
        private double[] densitySamples;
        @NonNull
        private List<double[]> intersections;
        @NonNull
        private Set<ProcessorId> participants;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .consumer(getConsumer())
                            .intersectionSegment(getIntersectionSegment())
                            .densitySamples(getDensitySamples())
                            .intersections(getIntersections())
                            .participants(getParticipants())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class NodeCollectionCreatedMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -5438441550645211965L;
        @NonNull
        private NodeCollection nodeCollection;
    }
}
