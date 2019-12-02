package de.hpi.msc.jschneider.fileHandling.reading;

import java.util.ArrayList;
import java.util.Collection;

public class NullSequenceReader implements SequenceReader
{
    public static SequenceReader get()
    {
        return new NullSequenceReader();
    }

    private NullSequenceReader()
    {
    }

    @Override
    public long getSize()
    {
        return 0;
    }

    @Override
    public boolean isNull()
    {
        return true;
    }

    @Override
    public Collection<? extends Float> read(long start, int length)
    {
        return new ArrayList<Float>();
    }

    @Override
    public SequenceReader subReader(long start, long length)
    {
        return NullSequenceReader.get();
    }
}
