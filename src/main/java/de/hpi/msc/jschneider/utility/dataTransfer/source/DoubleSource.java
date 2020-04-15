package de.hpi.msc.jschneider.utility.dataTransfer.source;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;

public class DoubleSource implements DataSource
{
    private final DoubleBigList data;
    private final DoubleIterator dataIterator;
    private final byte[] sendBuffer;
    private int sendBufferLength = 0;

    public DoubleSource(DoubleBigList data)
    {
        this(data, SystemParameters.getMaximumMessageSize());
    }

    public DoubleSource(DoubleBigList data, int maximumMessageSize)
    {
        this.data = data;
        dataIterator = data.iterator();
        sendBuffer = new byte[Calculate.nextSmallerMultipleOf((int) (maximumMessageSize * DataSource.MESSAGE_SIZE_SCALING_FACTOR), Double.BYTES)];
    }

    @Override
    public DataTransferMessages.DataTransferSynchronizationMessage createSynchronizationMessage(ActorRef sender, ActorRef receiver, long operationId)
    {
        return DataTransferMessages.DataTransferSynchronizationMessage.builder()
                                                                      .sender(sender)
                                                                      .receiver(receiver)
                                                                      .operationId(operationId)
                                                                      .bufferSize(sendBuffer.length)
                                                                      .numberOfElements(data.size64())
                                                                      .build();
    }

    @Override
    public boolean hasNext()
    {
        return !dataIterator.hasNext();
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
