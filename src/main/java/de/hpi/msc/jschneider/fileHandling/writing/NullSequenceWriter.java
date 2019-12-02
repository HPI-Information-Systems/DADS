package de.hpi.msc.jschneider.fileHandling.writing;

public class NullSequenceWriter implements SequenceWriter
{

    private NullSequenceWriter()
    {

    }

    public static SequenceWriter get()
    {
        return new NullSequenceWriter();
    }

    @Override
    public void write(Float[] records)
    {
    }

    @Override
    public boolean isNull()
    {
        return true;
    }

    @Override
    public void close()
    {
    }
}
