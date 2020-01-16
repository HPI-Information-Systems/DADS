package de.hpi.msc.jschneider.protocol.edgeCreation.worker;

import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.Int32Range;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@SuperBuilder
public class EdgeCreationWorkerModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private Int32Range localSegments;
    @Getter
    private final Map<Integer, IntersectionCollection> intersectionsInSegment = new HashMap<>();
    @Getter
    private final Map<Integer, NodeCollection> nodesInSegment = new HashMap<>();
}
