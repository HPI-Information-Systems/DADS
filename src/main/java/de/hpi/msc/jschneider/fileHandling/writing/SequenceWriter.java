package de.hpi.msc.jschneider.fileHandling.writing;

import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;

public interface SequenceWriter extends DataSink
{
    void write(float[] records);

    boolean isNull();

    void close();
}
