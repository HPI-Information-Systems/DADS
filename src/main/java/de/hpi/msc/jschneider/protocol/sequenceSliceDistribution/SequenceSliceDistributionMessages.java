package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class SequenceSliceDistributionMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeSequenceSliceTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = 5192009128359237303L;
        private int subSequenceLength;
        private int convolutionSize;
        private long firstSubSequenceIndex;
        private boolean isLastSubSequenceChunk;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .operationId(getOperationId())
                            .numberOfElements(getNumberOfElements())
                            .subSequenceLength(getSubSequenceLength())
                            .convolutionSize(getConvolutionSize())
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .isLastSubSequenceChunk(isLastSubSequenceChunk())
                            .build();
        }
    }
}
