package de.hpi.msc.jschneider.fileHandling;

public class NullSequenceReader implements SequenceReader
{
    public static SequenceReader get()
    {
        return new NullSequenceReader();
    }

    private NullSequenceReader()
    { }

    @Override
    public boolean hasNext()
    {
        return false;
    }

    @Override
    public Float next()
    {
        return 0.0f;
    }
}
