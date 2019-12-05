package de.hpi.msc.jschneider.protocol.principalComponentAnalysis.coordinator;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
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
    private final MutableInteger nextParticipantIndex = new MutableInteger(0);
    @NonNull @Getter
    private final Map<RootActorPath, Integer> participantIndices = new HashMap<>();

}
