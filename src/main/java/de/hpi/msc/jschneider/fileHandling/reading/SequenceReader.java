package de.hpi.msc.jschneider.fileHandling.reading;

import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;

public interface SequenceReader extends DataSource
{
    long getSize();

    long getPosition();

    boolean isNull();

    SequenceReader subReader(long start, long length);
}
