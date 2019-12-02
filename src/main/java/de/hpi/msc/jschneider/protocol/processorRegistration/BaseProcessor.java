package de.hpi.msc.jschneider.protocol.processorRegistration;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder @Getter
public class BaseProcessor implements Processor
{
    @NonNull
    private boolean isMaster;
    @NonNull
    private RootActorPath rootPath;
    @NonNull @Builder.Default
    private long maximumMemoryInBytes = SystemParameters.getMaximumMemory();
    @NonNull @Builder.Default
    private int numberOfThreads = SystemParameters.getNumberOfThreads();
    @NonNull
    private Protocol[] protocols;
}
