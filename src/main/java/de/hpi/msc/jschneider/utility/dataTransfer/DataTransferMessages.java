package de.hpi.msc.jschneider.utility.dataTransfer;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.UUID;

public class DataTransferMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class InitializeDataTransferMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 7385042862954704746L;
        private UUID operationId;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class DataPartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -4211856951220195380L;
        private UUID operationId;
        private float[] part;
        private boolean isLastPart;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class RequestNextDataPartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 53330389621278219L;
        private UUID operationId;
    }
}
