package de.hpi.msc.jschneider.utility.dataTransfer.source;

import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;

public class InPlaceDoublesSource implements DataSource
{
    private final DoubleCollection data;
    private final DoubleIterator dataIterator;
    private final byte[] sendBuffer;
    private int sendBufferSize = 0;

    public InPlaceDoublesSource(DoubleCollection data, int maximumMessageSize)
    {
        this.data = data;
        dataIterator = data.iterator();
        sendBuffer = new byte[Calculate.nextSmallerMultipleOf(maximumMessageSize, Double.BYTES)];
    }

    @Override
    public boolean isAtEnd()
    {
        return !dataIterator.hasNext();
    }

    @Override
    public int elementSizeInBytes()
    {
        return Double.BYTES;
    }

    @Override
    public int numberOfElements()
    {
        return data.size();
    }

    @Override
    public byte[] read(int maximumPartSize)
    {
        sendBufferSize = Serialize.inPlace(dataIterator, sendBuffer);
        return sendBuffer;
    }
}
