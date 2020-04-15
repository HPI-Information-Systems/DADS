package de.hpi.msc.jschneider.utility.dataTransfer.source;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.math.Intersection;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;

import java.util.Comparator;
import java.util.PrimitiveIterator;

public class IntersectionCollectionSource implements DataSource
{
    private final IntersectionCollection data;
    private final PrimitiveIterator.OfDouble dataIterator;
    private final byte[] sendBuffer;
    private int sendBufferLength;

    public IntersectionCollectionSource(IntersectionCollection data)
    {
        this(data, SystemParameters.getMaximumMessageSize());
    }

    public IntersectionCollectionSource(IntersectionCollection data, int maximumMessageSize)
    {
        this.data = data;
        dataIterator = data.getIntersections().stream()
                           .sorted(Comparator.comparingLong(Intersection::getCreationIndex))
                           .mapToDouble(Intersection::getIntersectionDistance)
                           .iterator();
        sendBuffer = new byte[Calculate.nextSmallerMultipleOf((int) (maximumMessageSize * DataSource.MESSAGE_SIZE_SCALING_FACTOR), Double.BYTES)];
    }

    @Override
    public DataTransferMessages.DataTransferSynchronizationMessage createSynchronizationMessage(ActorRef sender, ActorRef receiver, long operationId)
    {
        return DataTransferMessages.DataTransferSynchronizationMessage.builder()
                                                                      .sender(sender)
                                                                      .receiver(receiver)
                                                                      .operationId(operationId)
                                                                      .numberOfElements(data.getIntersections().size64())
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
