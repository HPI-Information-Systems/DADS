package de.hpi.msc.jschneider.actor.common.workDispatcher;

import de.hpi.msc.jschneider.actor.common.AbstractMessage;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class WorkDispatcherMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class RegisterAtMasterMessage extends AbstractMessage
    {
        private static final long serialVersionUID = 256722332989409844L;
        private String masterAddress;
    }

    @NoArgsConstructor @SuperBuilder
    public static class AcknowledgeRegistrationMessage extends AbstractMessage
    {
        private static final long serialVersionUID = 8412988323883201555L;
    }
}
