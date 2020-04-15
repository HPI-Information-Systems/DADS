package de.hpi.msc.jschneider.utility.dataTransfer;

import akka.actor.ActorRef;
import de.hpi.msc.jschneider.data.graph.GraphEdge;
import de.hpi.msc.jschneider.math.IntersectionCollection;
import de.hpi.msc.jschneider.math.NodeCollection;
import de.hpi.msc.jschneider.utility.dataTransfer.source.DoubleSource;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GraphEdgeSource;
import de.hpi.msc.jschneider.utility.dataTransfer.source.IntersectionCollectionSource;
import de.hpi.msc.jschneider.utility.dataTransfer.source.MatrixSource;
import de.hpi.msc.jschneider.utility.dataTransfer.source.NodeCollectionSource;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import org.ojalgo.matrix.store.MatrixStore;

public interface DataSource
{
    double MESSAGE_SIZE_SCALING_FACTOR = 0.25d;

    static DataSource create(DoubleBigList data)
    {
        return new DoubleSource(data);
    }

    static DataSource create(MatrixStore<Double> data)
    {
        return new MatrixSource(data);
    }

    static DataSource create(IntersectionCollection data)
    {
        return new IntersectionCollectionSource(data);
    }

    static DataSource create(NodeCollection data)
    {
        return new NodeCollectionSource(data);
    }

    static DataSource create(Iterable<GraphEdge> data)
    {
        return new GraphEdgeSource(data);
    }

    DataTransferMessages.DataTransferSynchronizationMessage createSynchronizationMessage(ActorRef sender, ActorRef receiver, long operationId);

    boolean hasNext();

    void next();

    byte[] buffer();

    int bufferLength();
}
