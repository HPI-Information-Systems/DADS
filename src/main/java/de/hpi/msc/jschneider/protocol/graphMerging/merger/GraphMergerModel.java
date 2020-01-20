package de.hpi.msc.jschneider.protocol.graphMerging.merger;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class GraphMergerModel extends AbstractProtocolParticipantModel
{
    @Getter
    private final Map<Integer, GraphEdge> edges = new HashMap<>();
}
