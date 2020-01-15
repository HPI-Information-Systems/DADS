package de.hpi.msc.jschneider.fileHandling.reading;

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
    public long getPosition()
    {
        return 0;
    }

    @Override
    public boolean isAtEnd()
    {
        return true;
    }

    @Override
    public boolean isNull()
    {
        return true;
    }

    @Override
    public float[] read(long length)
    {
        return new float[0];
    }

    @Override
    public float[] read(long start, long length)
    {
        return new float[0];
    }

    @Override
    public SequenceReader subReader(long start, long length)
    {
        return NullSequenceReader.get();
    }
}