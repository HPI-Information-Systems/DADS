package de.hpi.msc.jschneider.fileHandling;

import de.hpi.msc.jschneider.fileHandling.reading.SequenceReader;
import lombok.var;

import java.util.Arrays;

public class MockSequenceReader implements SequenceReader
{
    private final float[] values;

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
    public boolean isNull()
    {
        return false;
    }

    @Override
    public float[] read(long start, int length)
    {
        return Arrays.copyOfRange(values, (int) start, (int) start + length);
    }

    @Override
    public SequenceReader subReader(long start, long length)
    {
        return new MockSequenceReader(read(start, (int) length));
    }
}
