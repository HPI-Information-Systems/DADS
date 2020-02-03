package de.hpi.msc.jschneider.fileHandling;

import de.hpi.msc.jschneider.fileHandling.reading.SequenceReader;
import de.hpi.msc.jschneider.utility.Serialize;
import lombok.val;
import lombok.var;

import java.util.Arrays;

public class MockSequenceReader implements SequenceReader
{
    private final double[] values;
    private long currentPosition = 0L;

    public MockSequenceReader(int length)
    {
        values = new double[length];
        for (var i = 0; i < length; ++i)
        {
            values[i] = i;
        }
    }

    public MockSequenceReader(double[] values)
    {
        this.values = values;
    }

    @Override
    public long getSize()
    {
        return values.length;
    }

    @Override
    public long getPosition()
    {
        return currentPosition;
    }

    @Override
    public boolean isAtEnd()
    {
        return currentPosition >= values.length;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public int elementSizeInBytes()
    {
        return Double.BYTES;
    }

    @Override
    public byte[] read(int maximumPartSize)
    {
        val values = read(currentPosition, (long) Math.floor(maximumPartSize / (double) elementSizeInBytes()));
        currentPosition += values.length;
        return Serialize.toBytes(values);
    }

    @Override
    public double[] read(long start, long length)
    {
        var begin = Math.max(0, Math.min(values.length, start));
        var end = Math.max(begin, Math.min(values.length, start + length));

        return Arrays.copyOfRange(values, (int) begin, (int) end);
    }

    @Override
    public SequenceReader subReader(long start, long length)
    {
        return new MockSequenceReader(read(start, (int) length));
    }
}
