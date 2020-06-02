package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import de.hpi.msc.jschneider.utility.event.EventHandler;
import de.hpi.msc.jschneider.utility.event.EventImpl;

public class AdHocGraphEdgeSink implements DataSink
{
    private GraphEdge[] graphEdges;
    private int graphEdgesLength = 0;
    private final EventImpl<AdHocGraphEdgeSink> onDataPart = new EventImpl<>();

    public AdHocGraphEdgeSink whenDataPart(EventHandler<AdHocGraphEdgeSink> handler)
    {
        onDataPart.subscribe(handler);
        return this;
    }

    @Override
    public void synchronize(DataTransferMessages.DataTransferSynchronizationMessage message)
    {
        assert message.getBufferSize() % Serialize.GRAPH_EDGE_SIZE == 0 : "BufferSize % ElementSize != 0!";

        graphEdges = new GraphEdge[message.getBufferSize() / Serialize.GRAPH_EDGE_SIZE];
        graphEdgesLength = 0;
    }

    @Override
    public void write(byte[] part, int partLength)
    {
        graphEdgesLength = Serialize.backInPlace(part, partLength, graphEdges);
        onDataPart.invoke(this);
    }

    public GraphEdge[] edges()
    {
        return graphEdges;
    }

    public int edgesLength()
    {
        return graphEdgesLength;
    }

    @Override
    public void close()
    {

    }
}
