package de.hpi.msc.jschneider.protocol.processorRegistration;

import akka.actor.Address;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;

public class ProcessorRegistrationMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class RegisterAtMasterMessage implements Serializable
    {
        private static final long serialVersionUID = 2493096188225380178L;
        @NonNull
        private Address masterAddress;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class ProcessorRegistrationMessage implements Serializable
    {
        private static final long serialVersionUID = -8915923440656902337L;
        @NonNull
        private Processor processor;
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class AcknowledgeRegistrationMessage implements Serializable
    {
        private static final long serialVersionUID = 3881986201955442030L;
    }
}
