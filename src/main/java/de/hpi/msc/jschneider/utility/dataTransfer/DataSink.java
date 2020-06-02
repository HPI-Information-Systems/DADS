package de.hpi.msc.jschneider.utility.dataTransfer;

public interface DataSink
{
    void synchronize(DataTransferMessages.DataTransferSynchronizationMessage message);

    void write(byte[] part, int partLength);

    void close();
}
