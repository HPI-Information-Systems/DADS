package de.hpi.msc.jschneider.protocol.dimensionReduction;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

public class DimensionReductionEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ReducedProjectionCreatedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -5115390658605851185L;
        private MatrixStore<Double> reducedProjection;
        private long firstSubSequenceIndex;
        private boolean isLastSubSequenceChunk;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .reducedProjection(getReducedProjection())
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .isLastSubSequenceChunk(isLastSubSequenceChunk())
                            .build();
        }
    }
}
