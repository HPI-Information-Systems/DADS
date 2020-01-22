package de.hpi.msc.jschneider.utility.dataTransfer;

import de.hpi.msc.jschneider.protocol.ProtocolTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.GraphSink;
import de.hpi.msc.jschneider.utility.dataTransfer.source.GraphDataSource;
import lombok.val;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGraphDataTransfer extends ProtocolTestCase
{
    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[0];
    }

    public void testSourceAndSink()
    {
        val graph = createGraph(360, 180);
        val source = new GraphDataSource(graph);
        val initializationMessage = GraphDataSource.InitializeGraphTransferMessage.builder()
                                                                                  .sender(self.ref())
                                                                                  .receiver(self.ref())
                                                                                  .operationId(UUID.randomUUID())
                                                                                  .sizeOfEdgeCreationOrder(graph.getEdgeCreationOrder().size())
                                                                                  .sizeOfEdges(graph.getEdges().size())
                                                                                  .build();

        val sink = new GraphSink(initializationMessage);

        while (!source.isAtEnd())
        {
            sink.write(source.read(100));
        }
        sink.close();

        assertThat(sink.getGraph().getEdges()).isEqualTo(graph.getEdges());
        assertThat(sink.getGraph().getEdgeCreationOrder()).isEqualTo(graph.getEdgeCreationOrder());
    }
}
