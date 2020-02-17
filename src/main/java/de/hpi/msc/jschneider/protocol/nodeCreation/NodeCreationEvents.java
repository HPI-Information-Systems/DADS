package de.hpi.msc.jschneider.protocol.nodeCreation;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Map;

public class NodeCreationEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ResponsibilitiesReceivedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 8873046118928619016L;
        private Map<ProcessorId, Int32Range> segmentResponsibilities;
        private Map<ProcessorId, Int64Range> subSequenceResponsibilities;
        private int numberOfIntersectionSegments;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .segmentResponsibilities(getSegmentResponsibilities())
                            .subSequenceResponsibilities(getSubSequenceResponsibilities())
                            .numberOfIntersectionSegments(getNumberOfIntersectionSegments())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class IntersectionsCalculatedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -8226513489732567804L;
        private IntersectionCollection intersectionCollection;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .intersectionCollection(getIntersectionCollection())
                            .build();
        }
    }
}
