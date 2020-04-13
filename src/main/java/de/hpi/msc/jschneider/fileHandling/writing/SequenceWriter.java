package de.hpi.msc.jschneider.fileHandling.writing;

public interface SequenceWriter
{
    void write(double[] records);

    boolean isNull();

    void close();
}
