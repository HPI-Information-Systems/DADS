package de.hpi.msc.jschneider.protocol.dimensionReduction.distributor;

import de.hpi.msc.jschneider.protocol.common.Protocol;
import de.hpi.msc.jschneider.protocol.common.model.AbstractProtocolParticipantModel;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.ojalgo.matrix.store.MatrixStore;

@SuperBuilder
public class DimensionReductionDistributorModel extends AbstractProtocolParticipantModel
{
    @NonNull @Getter
    private Protocol receiverProtocol;
    @NonNull @Getter
    private MatrixStore<Double> rotation;
    @NonNull @Getter
    private MatrixStore<Double> principalComponents;
    @NonNull @Getter
    private MatrixStore<Double> columnMeans;
}
