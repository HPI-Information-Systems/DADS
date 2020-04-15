package de.hpi.msc.jschneider.fileHandling.writing;

import it.unimi.dsi.fastutil.doubles.DoubleBigList;

public interface SequenceWriter
{
    void write(DoubleBigList records);

    boolean isNull();

    void close();
}
