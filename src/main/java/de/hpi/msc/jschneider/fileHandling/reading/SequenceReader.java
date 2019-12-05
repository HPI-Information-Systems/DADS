package de.hpi.msc.jschneider.fileHandling.reading;

public interface SequenceReader
{
    long getSize();

    long getPosition();

    boolean isAtEnd();

    boolean isNull();

    float[] read(int length);

    float[] read(long start, int length);

    SequenceReader subReader(long start, long length);
}
