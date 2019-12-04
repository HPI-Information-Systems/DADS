package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.PrimitiveMatrix;

public class SequenceSliceDistributionEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ProjectionCreatedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 3362960763906428742L;
        private long firstSubSequenceIndex;
        private PrimitiveMatrix projectionSpace;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .projectionSpace(getProjectionSpace())
                            .build();
        }
    }
}
