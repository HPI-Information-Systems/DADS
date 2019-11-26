package de.hpi.msc.jschneider.actor.common.workDispatcher;

import akka.actor.Address;
import de.hpi.msc.jschneider.actor.common.AbstractCompletableMessage;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class WorkDispatcherMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class RegisterAtMasterMessage extends AbstractCompletableMessage
    {
        private static final long serialVersionUID = 256722332989409844L;
        private Address masterAddress;
    }

    @NoArgsConstructor @SuperBuilder
    public static class AcknowledgeRegistrationMessage extends AbstractCompletableMessage
    {
        private static final long serialVersionUID = 8412988323883201555L;
    }
}
