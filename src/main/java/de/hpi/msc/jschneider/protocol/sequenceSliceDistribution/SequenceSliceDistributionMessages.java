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
        private float[] slicePart;
        private int partIndex;
        private boolean isLastPart;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class AcknowledgeSequenceSlicePartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -7010936527056987490L;
    }
}
