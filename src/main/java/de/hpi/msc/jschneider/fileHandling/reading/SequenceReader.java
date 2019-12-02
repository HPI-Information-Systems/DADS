package de.hpi.msc.jschneider.fileHandling.reading;

public interface SequenceReader
{
    long getSize();

    boolean isNull();

    float[] read(long start, int length);

    SequenceReader subReader(long start, long length);
}
