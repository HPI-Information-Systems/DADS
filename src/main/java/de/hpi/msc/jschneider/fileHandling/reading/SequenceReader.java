package de.hpi.msc.jschneider.fileHandling.reading;

import java.util.Iterator;

public interface SequenceReader extends Iterator<Float>
{
    long getSize();

    boolean isNull();
}
