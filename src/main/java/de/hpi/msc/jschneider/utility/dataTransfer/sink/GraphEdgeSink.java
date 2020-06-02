package de.hpi.msc.jschneider.utility.dataTransfer.sink;

import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.utility.Serialize;
import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import de.hpi.msc.jschneider.utility.dataTransfer.DataTransferMessages;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

public class GraphEdgeSink implements DataSink
{
    private Int2ObjectMap<GraphEdge> edges;

    @Override
    public void synchronize(DataTransferMessages.DataTransferSynchronizationMessage message)
    {
        edges = new Int2ObjectLinkedOpenHashMap<>((int) Math.min(message.getNumberOfElements(), Integer.MAX_VALUE));
    }

    @Override
    public void write(byte[] part, int partLength)
    {
        Serialize.backInPlace(part, partLength, edges);
    }

    @Override
    public void close()
    {
    }

    public Int2ObjectMap<GraphEdge> edges()
    {
        return edges;
    }
}
