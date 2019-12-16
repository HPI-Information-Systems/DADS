package de.hpi.msc.jschneider.utility.dataTransfer;

public interface DataSource
{
    boolean isAtEnd();

    float[] read(long length);
}
