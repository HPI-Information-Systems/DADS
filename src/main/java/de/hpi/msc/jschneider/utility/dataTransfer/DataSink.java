package de.hpi.msc.jschneider.utility.dataTransfer;

public interface DataSink
{
    void write(float[] part);

    void close();
}
