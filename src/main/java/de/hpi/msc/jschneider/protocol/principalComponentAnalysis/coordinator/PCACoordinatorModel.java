package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.coordinator;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.Counter;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.agrona.collections.MutableInteger;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class PCACoordinatorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private int numberOfParticipants;
    @NonNull @Getter
    private final Counter nextParticipantIndex = new Counter(0);
    @NonNull @Getter
    private final Map<Long, RootActorPath> participantIndices = new HashMap<>();

}
