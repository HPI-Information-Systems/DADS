package de.hpi.msc.jschneider.utility.dataTransfer;

public interface DataSource
{
    boolean isAtEnd();

    int elementSizeInBytes();

    int numberOfElements();

    byte[] read(int maximumPartSize);
}
