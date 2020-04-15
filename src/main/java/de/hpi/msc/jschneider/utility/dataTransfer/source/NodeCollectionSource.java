package de.hpi.msc.jschneider.utility.dataTransfer.source;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;

import java.util.PrimitiveIterator;

public class NodeCollectionSource implements DataSource
{
    private final NodeCollection data;
    private final PrimitiveIterator.OfDouble dataIterator;
    private final byte[] sendBuffer;
    private int sendBufferLength;

    public NodeCollectionSource(NodeCollection data)
    {
        this(data, SystemParameters.getMaximumMessageSize());
    }

    public NodeCollectionSource(NodeCollection data, int maximumMessageSize)
    {
        this.data = data;
        dataIterator = data.getNodes().iterator();
        sendBuffer = new byte[Calculate.nextSmallerMultipleOf((int) (maximumMessageSize * DataSource.MESSAGE_SIZE_SCALING_FACTOR), Double.BYTES)];
    }

    @Override
    public DataTransferMessages.DataTransferSynchronizationMessage createSynchronizationMessage(ActorRef sender, ActorRef receiver, long operationId)
    {
        return DataTransferMessages.DataTransferSynchronizationMessage.builder()
                                                                      .sender(sender)
                                                                      .receiver(receiver)
                                                                      .operationId(operationId)
                                                                      .numberOfElements(data.getNodes().size())
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
