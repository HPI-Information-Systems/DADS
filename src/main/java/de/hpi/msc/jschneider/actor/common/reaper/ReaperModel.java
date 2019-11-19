package de.hpi.msc.jschneider.actor.common.reaper;

import de.hpi.msc.jschneider.actor.common.AbstractActorModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class ReaperModel extends AbstractActorModel
{
    @NonNull @Getter
    private Runnable terminateSystemCallback;
}
