package de.hpi.msc.jschneider.fileHandling;

import de.hpi.msc.jschneider.fileHandling.reading.SequenceReader;
import lombok.val;
import lombok.var;

import java.util.Arrays;

public class MockSequenceReader implements SequenceReader
{
    private final float[] values;
    private long currentPosition = 0L;

    public MockSequenceReader(int length)
    {
        values = new float[length];
        for (var i = 0; i < length; ++i)
        {
            values[i] = (float) i;
        }
    }

    public MockSequenceReader(float[] values)
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
    public float[] read(long length)
    {
        val values = read(currentPosition, length);
        currentPosition += values.length;
        return values;
    }

    @Override
    public float[] read(long start, long length)
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
