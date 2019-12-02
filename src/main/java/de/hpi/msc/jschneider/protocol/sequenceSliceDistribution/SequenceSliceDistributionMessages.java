package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class SequenceSliceDistributionMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class SequenceSlicePartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -5545762062266668319L;
        private Float[] slicePart;
        private int partIndex;
        private boolean isLastPart;
    }
}
