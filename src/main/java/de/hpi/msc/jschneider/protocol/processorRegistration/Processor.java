package de.hpi.msc.jschneider.protocol.processorRegistration;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.Protocol;

public interface Processor
{
    boolean isMaster();

    RootActorPath getRootPath();

    long getMaximumMemoryInBytes();

    int getNumberOfThreads();

    Protocol[] getProtocols();
}
