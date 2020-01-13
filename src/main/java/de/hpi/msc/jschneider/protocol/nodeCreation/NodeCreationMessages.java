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
                            .subSequenceIndices(getSubSequenceIndices())
                            .isLastSubSequenceChunk(isLastSubSequenceChunk())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeNodeCreationMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 1734911241966860272L;
        private int numberOfSamples;
        private double maximumValue;
        private Map<ActorRef, Int32Range> sampleResponsibilities;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class IntersectionsAtAngleMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -8012069364274224581L;
        private int intersectionPointIndex;
        private float[] intersections;
    }
}
