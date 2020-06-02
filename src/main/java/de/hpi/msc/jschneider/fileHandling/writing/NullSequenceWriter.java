package de.hpi.msc.jschneider.fileHandling.writing;

import it.unimi.dsi.fastutil.doubles.DoubleBigList;

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
    public void write(DoubleBigList records)
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
