package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

public class SequenceSliceDistributionMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeSequenceSliceTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = 5192009128359237303L;
        @NonNull
        private int subSequenceLength;
        @NonNull
        private int convolutionSize;
        @NonNull
        private long firstSubSequenceIndex;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .operationId(getOperationId())
                            .subSequenceLength(getSubSequenceLength())
                            .convolutionSize(getConvolutionSize())
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .build();
        }
    }
}
