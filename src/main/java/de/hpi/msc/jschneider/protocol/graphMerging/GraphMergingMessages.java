package de.hpi.msc.jschneider.protocol.graphMerging;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

public class GraphMergingMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeEdgePartitionTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = 240768917136336270L;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .operationId(getOperationId())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class EdgesReceivedMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -7844535127694995217L;
        private GraphEdge[] edges;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class AllEdgesReceivedMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = -7352521533354529747L;
        @NonNull
        private ProcessorId[] workerSystems;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeGraphTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = -2304301282642362495L;

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
