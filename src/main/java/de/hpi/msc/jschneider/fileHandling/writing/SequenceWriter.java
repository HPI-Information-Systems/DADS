package de.hpi.msc.jschneider.fileHandling.writing;

public interface SequenceWriter
{
    void write(float[] records);

    boolean isNull();

    void close();
}
