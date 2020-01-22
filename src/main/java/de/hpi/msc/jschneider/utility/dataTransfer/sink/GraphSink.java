package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.data.graph.Graph;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GraphDataSource;
import lombok.val;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GraphSink implements DataSink
{
    private static final Logger Log = LogManager.getLogger(GraphSink.class);


    private GraphEdge[] edges;
    private int edgesIndex = 0;
    private int[] edgeCreationOrder;
    private int edgeCreationOderIndex = 0;
    private Graph graph;

    public GraphSink(GraphDataSource.InitializeGraphTransferMessage initializeGraphTransferMessage)
    {
        edges = new GraphEdge[initializeGraphTransferMessage.getSizeOfEdges()];
        edgeCreationOrder = new int[initializeGraphTransferMessage.getSizeOfEdgeCreationOrder()];
    }

    @Override
    public void write(byte[] part)
    {
        if (edgesIndex < edges.length)
        {
            writeEdges(part);
            return;
        }

        if (edgeCreationOderIndex < edgeCreationOrder.length)
        {
            writeEdgeCreationOrder(part);
            return;
        }

        Log.error("Received too many bytes!");
    }

    private void writeEdges(byte[] part)
    {
        val newEdges = Serialize.toGraphEdges(part);
        System.arraycopy(newEdges, 0, edges, edgesIndex, newEdges.length);
        edgesIndex += newEdges.length;
    }

    private void writeEdgeCreationOrder(byte[] part)
    {
        val newEntries = Serialize.toInts(part);
        System.arraycopy(newEntries, 0, edgeCreationOrder, edgeCreationOderIndex, newEntries.length);
        edgeCreationOderIndex += newEntries.length;
    }

    @Override
    public void close()
    {
        graph = Graph.construct(edges, edgeCreationOrder);
    }

    public Graph getGraph()
    {
        return graph;
    }
}
