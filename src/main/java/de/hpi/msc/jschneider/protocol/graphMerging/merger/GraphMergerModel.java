package de.hpi.msc.jschneider.protocol.graphMerging.merger;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.utility.Int64Range;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class GraphMergerModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private Map<ProcessorId, Int64Range> subSequenceResponsibilities;
    @Getter
    private final Map<Integer, GraphEdge> edges = new HashMap<>();
    @Setter @Getter
    private LongSet runningGraphTransfers;
}
