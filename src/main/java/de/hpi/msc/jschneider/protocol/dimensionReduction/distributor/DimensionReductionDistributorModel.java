package de.hpi.msc.jschneider.protocol.dimensionReduction.distributor;

import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

@SuperBuilder
public class DimensionReductionDistributorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private Protocol receiverProtocol;
    @NonNull @Getter
    private MatrixStore<Double> rotation;
    @Setter @Getter
    private long rotationTransferOperationId;
    @Setter @Getter @Builder.Default
    private boolean rotationTransferCompleted = false;
    @NonNull @Getter
    private MatrixStore<Double> principalComponents;
    @Setter @Getter
    private long principalComponentsTransferOperationId;
    @Setter @Getter @Builder.Default
    private boolean principalComponentsTransferCompleted = false;
    @NonNull @Getter
    private MatrixStore<Double> columnMeans;
    @Setter @Getter
    private long columnMeansTransferOperationId;
    @Setter @Getter @Builder.Default
    private boolean columnMeansTransferCompleted = false;
}
