package de.hpi.msc.jschneider.protocol.nodeCreation;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.Int32Range;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Map;

public class NodeCreationMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class NodeCreationWorkerReadyMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 1084869085409658319L;
        private Int64Range subSequenceIndices;
        private double maximumValue;
        private boolean isLastSubSequenceChunk;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .subSequenceIndices(getSubSequenceIndices())
                            .maximumValue(getMaximumValue())
                            .isLastSubSequenceChunk(isLastSubSequenceChunk())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeNodeCreationMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 1734911241966860272L;
        private int numberOfIntersectionSegments;
        private double maximumValue;
        private Map<ActorRef, Int32Range> intersectionSegmentResponsibilities;
        private Map<ActorRef, Int64Range> subSequenceResponsibilities;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .numberOfIntersectionSegments(getNumberOfIntersectionSegments())
                            .maximumValue(getMaximumValue())
                            .intersectionSegmentResponsibilities(getIntersectionSegmentResponsibilities())
                            .subSequenceResponsibilities(getSubSequenceResponsibilities())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class ReducedSubSequenceMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 7735483469077801832L;
        private long subSequenceIndex;
        private double subSequenceX;
        private double subSequenceY;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class IntersectionsMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -8012069364274224581L;
        private int intersectionSegment;
        private double[] intersections;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class NodesMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 2030668695666469041L;
        private int intersectionSegment;
        private double[] nodes;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .intersectionSegment(getIntersectionSegment())
                            .nodes(getNodes())
                            .build();
        }
    }
}
