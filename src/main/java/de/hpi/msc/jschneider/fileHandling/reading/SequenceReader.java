package de.hpi.msc.jschneider.fileHandling.reading;

import java.util.Collection;

public interface SequenceReader
{
    long getSize();

    boolean isNull();

    Collection<? extends Float> read(long start, int length);

    SequenceReader subReader(long start, long length);
}
