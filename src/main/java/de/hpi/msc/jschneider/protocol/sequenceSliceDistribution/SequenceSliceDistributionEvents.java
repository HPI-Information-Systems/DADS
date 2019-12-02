package de.hpi.msc.jschneider.protocol.sequenceSliceDistribution;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class SequenceSliceDistributionEvents
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class SequenceSlicePartReceivedEvent extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 5976452976979532797L;
        private float[] slicePart;
    }
}
