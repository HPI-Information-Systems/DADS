package de.hpi.msc.jschneider.protocol.processorRegistration;

import de.hpi.msc.jschneider.SystemParameters;
import de.hpi.msc.jschneider.protocol.common.Protocol;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @AllArgsConstructor @Builder @Getter
public class BaseProcessor implements Processor
{
    private boolean isMaster;
    private ProcessorId id;
    @Builder.Default
    private long maximumMemoryInBytes = SystemParameters.getMaximumMemory();
    @Builder.Default
    private int numberOfThreads = SystemParameters.getNumberOfThreads();
    @Setter
    private Protocol[] protocols;
}
