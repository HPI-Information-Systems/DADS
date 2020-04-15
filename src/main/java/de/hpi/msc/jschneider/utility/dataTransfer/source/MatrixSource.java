package de.hpi.msc.jschneider.utility.dataTransfer.source;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.utility.MatrixIterator;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import org.ojalgo.matrix.store.MatrixStore;

public class MatrixSource implements DataSource
{
    private final MatrixStore<Double> data;
    private final MatrixIterator dataIterator;
    private final byte[] sendBuffer;
    private int sendBufferLength = 0;

    public MatrixSource(MatrixStore<Double> data)
    {
        this(data, SystemParameters.getMaximumMessageSize());
    }

    public MatrixSource(MatrixStore<Double> data, int maximumMessageSize)
    {
        this.data = data;
        dataIterator = new MatrixIterator(data);
        sendBuffer = new byte[Calculate.nextSmallerMultipleOf((int) (maximumMessageSize * DataSource.MESSAGE_SIZE_SCALING_FACTOR), Double.BYTES)];
    }

    @Override
    public DataTransferMessages.DataTransferSynchronizationMessage createSynchronizationMessage(ActorRef sender, ActorRef receiver, long operationId)
    {
        return DataTransferMessages.MatrixTransferSynchronizationMessage.builder()
                                                                        .sender(sender)
                                                                        .receiver(receiver)
                                                                        .operationId(operationId)
                                                                        .numberOfElements(data.count())
                                                                        .numberOfRows(data.countRows())
                                                                        .numberOfColumns(data.countColumns())
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
