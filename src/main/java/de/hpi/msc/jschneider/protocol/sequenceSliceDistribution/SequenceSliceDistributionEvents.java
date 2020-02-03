package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

public class SequenceSliceDistributionEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class ProjectionCreatedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 3362960763906428742L;
        private long firstSubSequenceIndex;
        private boolean isLastSubSequenceChunk;
        private double minimumRecord;
        private double maximumRecord;
        private MatrixStore<Double> projection;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .isLastSubSequenceChunk(isLastSubSequenceChunk())
                            .minimumRecord(getMinimumRecord())
                            .maximumRecord(getMaximumRecord())
                            .projection(getProjection())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class SubSequenceParametersReceivedEvent extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 6340788448981144705L;
        private int subSequenceLength;
        private int convolutionSize;
        private long firstSubSequenceIndex;
        private boolean isLastSubSequenceChunk;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .subSequenceLength(getSubSequenceLength())
                            .convolutionSize(getConvolutionSize())
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .isLastSubSequenceChunk(isLastSubSequenceChunk())
                            .build();
        }
    }
}
