package de.hpi.msc.jschneider.utility.dataTransfer;

public interface DataSource
{
    boolean isAtEnd();

    int elementSizeInBytes();

    byte[] read(int maximumPartSize);
}
