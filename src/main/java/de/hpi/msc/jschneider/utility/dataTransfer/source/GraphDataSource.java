package de.hpi.msc.jschneider.utility.dataTransfer.source;

import akka.actor.ActorRef;
import com.google.common.primitives.Ints;
import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.SuperBuilder;
import lombok.val;

import java.util.HashMap;
import java.util.Map;

public class GraphDataSource implements DataSource
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class InitializeGraphTransferMessage extends DataTransferMessages.InitializeDataTransferMessage
    {
        private static final long serialVersionUID = 3224353644993576527L;
        private int sizeOfEdges;
        private int sizeOfEdgeCreationOrder;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .operationId(getOperationId())
                            .sizeOfEdges(getSizeOfEdges())
                            .sizeOfEdgeCreationOrder(getSizeOfEdgeCreationOrder())
                            .build();
        }
    }

    private enum TransferState
    {
        TransferEdges,
        TransferEdgeCreationOrder,
    }

    private boolean isAtEnd = false;
    private final Map<TransferState, DataSource> dataSources = new HashMap<>();
    private TransferState state;

    public GraphDataSource(Graph graph)
    {
        state = TransferState.TransferEdges;

        dataSources.put(TransferState.TransferEdges, GenericDataSource.create(graph.getEdges().values().toArray(new GraphEdge[0])));
        dataSources.put(TransferState.TransferEdgeCreationOrder, GenericDataSource.create(Ints.toArray(graph.getEdgeCreationOrder())));
    }

    @Override
    public boolean isAtEnd()
    {
        return isAtEnd;
    }

    @SneakyThrows
    @Override
    public int elementSizeInBytes()
    {
        return dataSources.get(state).elementSizeInBytes();
    }

    @Override
    public byte[] read(int maximumPartSize)
    {
        val bytes = dataSources.get(state).read(maximumPartSize);

        if (!dataSources.get(state).isAtEnd())
        {
            return bytes;
        }

        if (state == TransferState.TransferEdgeCreationOrder)
        {
            isAtEnd = true;
            return bytes;
        }

        state = TransferState.TransferEdgeCreationOrder;
        return bytes;
    }
}
