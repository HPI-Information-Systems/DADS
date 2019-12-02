package de.hpi.msc.jschneider.fileHandling.writing;

public interface SequenceWriter
{
    void write(Float[] records);

    boolean isNull();

    void close();
}
