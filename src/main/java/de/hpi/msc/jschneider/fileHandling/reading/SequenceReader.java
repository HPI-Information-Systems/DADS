package de.hpi.msc.jschneider.fileHandling.reading;

import de.hpi.msc.jschneider.utility.dataTransfer.DataSource;

public interface SequenceReader extends DataSource
{
    long getSize();

    long getPosition();

    boolean isAtEnd();

    boolean isNull();

    double[] read(long start, long length);

    SequenceReader subReader(long start, long length);
}
