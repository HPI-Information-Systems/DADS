package de.hpi.msc.jschneider.utility.dataTransfer;

import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

public class DataTransferMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static abstract class InitializeDataTransferMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 7385042862954704746L;
        @NonNull
        private int operationId;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class DataPartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -4211856951220195380L;
        @NonNull
        private int operationId;
        @NonNull
        private byte[] part;
        @NonNull
        private boolean isLastPart;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class RequestNextDataPartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 53330389621278219L;
        @NonNull
        private int operationId;
    }
}
