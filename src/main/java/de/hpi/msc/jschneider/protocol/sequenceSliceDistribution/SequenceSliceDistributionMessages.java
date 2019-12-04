package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class SequenceSliceDistributionMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeSliceTransferMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 5192009128359237303L;
        private int subSequenceLength;
        private int convolutionSize;
        private long firstSubSequenceIndex;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .subSequenceLength(getSubSequenceLength())
                            .convolutionSize(getConvolutionSize())
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class SequenceSlicePartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -5545762062266668319L;
        private float[] slicePart;
        private boolean isLastPart;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class RequestNextSlicePartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -7010936527056987490L;
    }
}
