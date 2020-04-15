package de.hpi.msc.jschneider.protocol.edgeCreation.worker.graphPartitionCreator;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.data.graph.GraphNode;
import de.hpi.msc.jschneider.protocol.actorPool.ActorPoolMessages;
import de.hpi.msc.jschneider.protocol.edgeCreation.worker.LocalIntersection;
import de.hpi.msc.jschneider.protocol.messageExchange.MessageExchangeMessages;
import de.hpi.msc.jschneider.utility.Int64Range;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

public class GraphPartitionCreatorMessages
{
    @NoArgsConstructor @SuperBuilder @Getter
    public static class CreateGraphPartitionMessage extends ActorPoolMessages.WorkMessage
    {
        private static final long serialVersionUID = -3571935829433975914L;
        @NonNull
        private List<LocalIntersection> intersections;
        @NonNull
        private Map<Integer, DoubleBigList> nodes;
        private long firstSubSequenceIndex;
        private GraphNode lastNode;
        @NonNull
        private Int64Range localSubSequences;
        private boolean lastChunk;

        @Override
        public MessageExchangeMessages.RedirectableMessage redirectTo(ActorRef newReceiver)
        {
            return builder().sender(getSender())
                            .receiver(newReceiver)
                            .forwarder(getReceiver())
                            .consumer(getConsumer())
                            .intersections(getIntersections())
                            .nodes(getNodes())
                            .firstSubSequenceIndex(getFirstSubSequenceIndex())
                            .lastNode(getLastNode())
                            .localSubSequences(getLocalSubSequences())
                            .lastChunk(isLastChunk())
                            .build();
        }
    }

    @NoArgsConstructor @SuperBuilder @Getter
    public static class GraphPartitionCreatedMessage extends MessageExchangeMessages.MessageExchangeMessage
    {
        private static final long serialVersionUID = 1657941577857588020L;
        private long firstIntersectionCreationIndex;
        @NonNull
        private Graph graphPartition;
    }
}
