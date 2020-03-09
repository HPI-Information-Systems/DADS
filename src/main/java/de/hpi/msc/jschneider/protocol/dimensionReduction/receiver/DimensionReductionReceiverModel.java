package de.hpi.msc.jschneider.protocol.dimensionReduction.receiver;

import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import de.hpi.msc.jschneider.utility.dataTransfer.sink.PrimitiveMatrixSink;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

@SuperBuilder
public class DimensionReductionReceiverModel extends AbstractProtocolParticipantModel
{
    @Setter @Getter
    private MatrixStore<Double> projection;
    @Setter @Getter
    private long firstSubSequenceIndex;
    @Setter @Getter
    private boolean isLastSubSequenceChunk;
    @Getter
    private final PrimitiveMatrixSink principalComponentsSink = new PrimitiveMatrixSink();
    @Setter @Getter
    private MatrixStore<Double> principalComponents;
    @Getter
    private final PrimitiveMatrixSink rotationSink = new PrimitiveMatrixSink();
    @Setter @Getter
    private MatrixStore<Double> rotation;
    @Setter @Getter
    private long numberOfColumns;
    @Getter
    private final PrimitiveMatrixSink columnMeansSink = new PrimitiveMatrixSink();
    @Setter @Getter
    private MatrixStore<Double> columnMeans;
}
