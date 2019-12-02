package de.hpi.msc.jschneider.protocol.reaper.reaper;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class ReaperModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter @Setter
    private Runnable terminateActorSystemCallback;
}
