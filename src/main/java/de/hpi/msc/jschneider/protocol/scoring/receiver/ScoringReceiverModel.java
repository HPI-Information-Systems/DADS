package de.hpi.msc.jschneider.protocol.scoring.receiver;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Int64Range;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuperBuilder
public class ScoringReceiverModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter @Builder.Default
    private final Set<ProcessorId> runningDataTransfers = new HashSet<>();
    @Setter @Getter
    private Map<ProcessorId, Int64Range> subSequenceResponsibilities;
    @NonNull @Getter
    private final Object2ObjectMap<ProcessorId, DoubleBigList> pathScores = new Object2ObjectLinkedOpenHashMap<>();
    @Setter @Getter @Builder.Default
    private boolean responsibilitiesReceived = false;
}
