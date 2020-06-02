package de.hpi.msc.jschneider.protocol.graphMerging.receiver;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.GraphEdgeSink;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public class GraphReceiverModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private final GraphEdgeSink graphEdgeSink = new GraphEdgeSink();
}
