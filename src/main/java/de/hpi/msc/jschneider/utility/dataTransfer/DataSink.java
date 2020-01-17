package de.hpi.msc.jschneider.utility.dataTransfer;

public interface DataSink
{
    void write(byte[] part);

    void close();
}
