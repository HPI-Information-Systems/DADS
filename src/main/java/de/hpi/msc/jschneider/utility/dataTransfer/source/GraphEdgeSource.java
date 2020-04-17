package de.hpi.msc.jschneider.utility.dataTransfer.source;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;

import java.util.Iterator;
import java.util.stream.StreamSupport;

public class GraphEdgeSource implements DataSource
{
    private final long numberOfElements;
    private final Iterator<GraphEdge> dataIterator;
    private final byte[] sendBuffer;
    private int sendBufferLength;

    public GraphEdgeSource(Iterable<GraphEdge> data)
    {
        this(data, SystemParameters.getMaximumMessageSize());
    }

    public GraphEdgeSource(Iterable<GraphEdge> data, int maximumMessageSize)
    {
        numberOfElements = StreamSupport.stream(data.spliterator(), false).count();
        dataIterator = data.iterator();
        sendBuffer = new byte[Calculate.nextSmallerMultipleOf((int) (maximumMessageSize * DataSource.MESSAGE_SIZE_SCALING_FACTOR), Serialize.GRAPH_EDGE_SIZE)];
    }

    @Override
    public DataTransferMessages.DataTransferSynchronizationMessage createSynchronizationMessage(ActorRef sender, ActorRef receiver, long operationId)
    {
        return DataTransferMessages.DataTransferSynchronizationMessage.builder()
                                                                      .sender(sender)
                                                                      .receiver(receiver)
                                                                      .operationId(operationId)
                                                                      .numberOfElements(numberOfElements)
                                                                      .bufferSize(sendBuffer.length)
                                                                      .build();
    }

    @Override
    public boolean hasNext()
    {
        return dataIterator.hasNext();
    }

    @Override
    public void next()
    {
        sendBufferLength = Serialize.inPlace(dataIterator, sendBuffer);
    }

    @Override
    public byte[] buffer()
    {
        return sendBuffer;
    }

    @Override
    public int bufferLength()
    {
        return sendBufferLength;
    }
}
