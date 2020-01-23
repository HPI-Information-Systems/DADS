package de.hpi.msc.jschneider.protocol.scoring.receiver;

import akka.actor.RootActorPath;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.Int64Range;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuperBuilder
public class ScoringReceiverModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private int subSequenceLength;
    @Setter @Getter
    private Set<RootActorPath> runningDataTransfers;
    @Setter @Getter
    private Map<RootActorPath, Int64Range> subSequenceResponsibilities;
    @NonNull @Getter
    private final Map<RootActorPath, float[]> pathScores = new HashMap<>();
    @Setter @Getter @Builder.Default
    private boolean responsibilitiesReceived = false;
}
