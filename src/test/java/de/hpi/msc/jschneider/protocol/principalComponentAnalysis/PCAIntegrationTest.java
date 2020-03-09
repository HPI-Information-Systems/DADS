package de.hpi.msc.jschneider.protocol.principalComponentAnalysis;

import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.BaseTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator.PCACalculatorControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator.PCACalculatorModel;
import de.hpi.msc.jschneider.protocol.processorRegistration.ProcessorId;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.utility.matrix.RowMatrixBuilder;
import lombok.val;
import lombok.var;
import org.ojalgo.data.DataProcessors;
import org.ojalgo.function.aggregator.Aggregator;
import org.ojalgo.matrix.decomposition.Eigenvalue;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class PCAIntegrationTest extends BaseTestCase
{
    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.PrincipalComponentAnalysis};
    }

    private SequenceSliceDistributionEvents.ProjectionCreatedEvent sendProjectionCreatedEvent(TestProbe receiver, PartialFunction<Object, BoxedUnit> messageInterface, MatrixStore<Double> projection)
    {
        val min = projection.aggregateAll(Aggregator.MINIMUM);
        val max = projection.aggregateAll(Aggregator.MAXIMUM);

        val event = SequenceSliceDistributionEvents.ProjectionCreatedEvent.builder()
                                                                          .receiver(receiver.ref())
                                                                          .sender(receiver.ref())
                                                                          .firstSubSequenceIndex(0L)
                                                                          .minimumRecord(min)
                                                                          .maximumRecord(max)
                                                                          .projection(projection)
                                                                          .build();
        messageInterface.apply(event);

        return event;
    }

    private void comparePCA(MatrixStore<Double> principalComponents, Eigenvalue<Double> evd)
    {
        for (var i = 0; i < principalComponents.countColumns(); ++i)
        {
            val svdColumn = principalComponents.logical().column(i).get();
            val evdColumn = evd.getV().logical().column(i).get();

            val isEqual =
                    svdColumn.equals(evdColumn, MATRIX_COMPARISON_CONTEXT) ||
                    svdColumn.equals(evdColumn.multiply(-1.0d), MATRIX_COMPARISON_CONTEXT);

            assertThat(isEqual).isTrue();
        }
    }

    public void testCalculateDistributedPCA()
    {
        // initialize processors
        val master = createMaster();
        val slave = createSlave();
        val processorIndices = new HashMap<Long, ProcessorId>();
        processorIndices.put(0L, master.getId());
        processorIndices.put(1L, slave.getId());

        // initialize master actor
        val masterSelf = master.createActor("self");
        val masterModel = finalizeModel(PCACalculatorModel.builder()
                                                          .processorIndices(processorIndices)
                                                          .myProcessorIndex(0L)
                                                          .convolutionSize(25)
                                                          .build(), masterSelf);
        val masterControl = new PCACalculatorControl(masterModel);
        val masterMessageInterface = createMessageInterface(masterControl);

        // initialize slave actor
        val slaveSelf = slave.createActor("self");
        val slaveModel = finalizeModel(PCACalculatorModel.builder()
                                                         .processorIndices(processorIndices)
                                                         .myProcessorIndex(1L)
                                                         .convolutionSize(0)
                                                         .build(), slaveSelf);
        val slaveControl = new PCACalculatorControl(slaveModel);
        val slaveMessageInterface = createMessageInterface(slaveControl);

        // create projections
        val masterProjection = createMatrix(100, 5);
        val slaveProjection = createMatrix(100, 5);
        val projection = (new RowMatrixBuilder(masterProjection.countColumns()).append(masterProjection)
                                                                               .append(slaveProjection)
                                                                               .build());
        val projectionMin = projection.aggregateAll(Aggregator.MINIMUM);
        val projectionMax = projection.aggregateAll(Aggregator.MAXIMUM);

        // calculate expected PCA
        val covariances = DataProcessors.covariances(PrimitiveDenseStore.FACTORY, projection);
        val evd = Eigenvalue.PRIMITIVE.make(covariances);
        evd.decompose(covariances);

        // kick-off master PCA calculation
        val masterProjectionCreatedEvent = sendProjectionCreatedEvent(masterSelf, masterMessageInterface, masterProjection);
        assertThatMessageIsCompleted(masterProjectionCreatedEvent, master);

        // kick-off slave PCA calculation
        val slaveProjectionCreatedEvent = sendProjectionCreatedEvent(slaveSelf, slaveMessageInterface, slaveProjection);
        val initializeColumnMeansTransfer = slave.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(PCAMessages.InitializeColumnMeansTransferMessage.class);
        val initializeRTransfer = slave.getProtocolRootActor(ProtocolType.MessageExchange).expectMsgClass(PCAMessages.InitializeRTransferMessage.class);
        assertThatMessageIsCompleted(slaveProjectionCreatedEvent, slave);

        // send slave column means
        val slaveColumnMeans = performDataTransfer(masterSelf, masterMessageInterface,
                                                   slaveSelf, slaveMessageInterface,
                                                   (PCAMessages.InitializeColumnMeansTransferMessage) initializeColumnMeansTransfer.redirectTo(masterSelf.ref()),
                                                   true).getMatrix(slaveProjection.countColumns());
        assertThat(slaveColumnMeans.equals(Calculate.transposedColumnMeans(slaveProjection), MATRIX_COMPARISON_CONTEXT)).isTrue();

        // send slave R
        val slaveR = performDataTransfer(masterSelf, masterMessageInterface,
                                         slaveSelf, slaveMessageInterface,
                                         (PCAMessages.InitializeRTransferMessage) initializeRTransfer.redirectTo(masterSelf.ref()),
                                         false).getMatrix(slaveProjection.countColumns());
        assertThat(masterModel.getMinimumRecord()).isEqualTo(projectionMin);
        assertThat(masterModel.getMaximumRecord()).isEqualTo(projectionMax);

        // expect master to finish calculation
        val principalComponentsCreatedEvent = expectEvent(PCAEvents.PrincipalComponentsCreatedEvent.class, master);

        comparePCA(principalComponentsCreatedEvent.getPrincipalComponents(), evd);
    }

    public void testCalculatePCAOnMasterOnly()
    {
        val master = createMaster();
        val self = master.createActor("self");
        val model = finalizeModel(PCACalculatorModel.builder().build(), self);
        model.setProcessorIndices(new HashMap<>());
        model.getProcessorIndices().put(0L, master.getId());
        model.setMyProcessorIndex(0L);
        val control = new PCACalculatorControl(model);
        val messageInterface = createMessageInterface(control);

        val projection = createMatrix(100, 5);
        val projectionMin = projection.aggregateAll(Aggregator.MINIMUM);
        val projectionMax = projection.aggregateAll(Aggregator.MAXIMUM);
        val covariances = DataProcessors.covariances(PrimitiveDenseStore.FACTORY, projection);
        val evd = Eigenvalue.PRIMITIVE.make(covariances);
        evd.decompose(covariances);

        val projectionCreatedEvent = sendProjectionCreatedEvent(self, messageInterface, projection);
        assertThat(model.getMinimumRecord()).isEqualTo(projectionMin);
        assertThat(model.getMaximumRecord()).isEqualTo(projectionMax);
        val principalComponentsCreatedEvent = expectEvent(PCAEvents.PrincipalComponentsCreatedEvent.class, master);

        comparePCA(principalComponentsCreatedEvent.getPrincipalComponents(), evd);

        assertThatMessageIsCompleted(projectionCreatedEvent, master);
    }
}
