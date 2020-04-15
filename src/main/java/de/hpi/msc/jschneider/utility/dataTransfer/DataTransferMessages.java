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
        private long operationId;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class RequestDataTransferSynchronizationMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 2494274760654501310L;
        private long operationId;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class DataTransferSynchronizationMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 5673457957093341350L;
        private long operationId;
        private long numberOfElements;
        private int bufferSize;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class MatrixTransferSynchronizationMessage extends DataTransferSynchronizationMessage
    {
        private static final long serialVersionUID = -5071371382988058267L;
        private long numberOfRows;
        private long numberOfColumns;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class DataPartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -4211856951220195380L;
        private long operationId;
        @NonNull
        private byte[] part;
        private int partLength;
        private boolean isLastPart;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class RequestNextDataPartMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 53330389621278219L;
        private long operationId;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class DataTransferFinishedMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -2105181322147039111L;
        private long operationId;
    }
}
