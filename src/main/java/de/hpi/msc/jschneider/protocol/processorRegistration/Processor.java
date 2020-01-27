package de.hpi.msc.jschneider.protocol.processorRegistration;

import de.hpi.msc.jschneider.protocol.common.Protocol;

public interface Processor
{
    boolean isMaster();

    ProcessorId getId();

    long getMaximumMemoryInBytes();

    int getNumberOfThreads();

    Protocol[] getProtocols();
}
