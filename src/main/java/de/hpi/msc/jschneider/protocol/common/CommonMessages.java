package de.hpi.msc.jschneider.protocol.common;

import de.hpi.msc.jschneider.protocol.processorRegistration.Processor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

public class CommonMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class SetUpProtocolMessage implements Serializable
    {
        private static final long serialVersionUID = -4641719443989328434L;
        private Processor localProcessor;
    }
}
