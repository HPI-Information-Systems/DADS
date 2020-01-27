package de.hpi.msc.jschneider.protocol.scoring;

import akka.actor.ActorRef;
import com.esotericsoftware.kryo.NotNull;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;

public class ScoringMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class QueryPathLengthMessage extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = 1117346665010797573L;
        private int queryPathLength;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .queryPathLength(getQueryPathLength())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class OverlappingEdgeCreationOrder extends MessageExchangeMessages.RedirectableMessage
    {
        private static final long serialVersionUID = -5955687701293096759L;
        @NotNull
        private int[][] overlappingEdgeCreationOrder;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .overlappingEdgeCreationOrder(getOverlappingEdgeCreationOrder())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializePathScoresTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = 6054549294608329361L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .operationId(getOperationId())
                            .build();
        }
    }
}