package de.hpi.msc.jschneider.protocol.principalComponentAnalysis;

import akka.actor.RootActorPath;
import akka.testkit.TestProbe;
import de.hpi.msc.jschneider.math.Calculate;
import de.hpi.msc.jschneider.protocol.BaseTestCase;
import de.hpi.msc.jschneider.protocol.common.ProtocolType;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator.PCACalculatorControl;
import de.hpi.msc.jschneider.protocol.principalComponentAnalysis.calculator.PCACalculatorModel;
import de.hpi.msc.jschneider.protocol.sequenceSliceDistribution.SequenceSliceDistributionEvents;
import de.hpi.msc.jschneider.utility.MatrixInitializer;
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
                                                                          .minimumRecord(min.floatValue())
                                                                          .maximumRecord(max.floatValue())
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
        val processorIndices = new HashMap<Long, RootActorPath>();
        processorIndices.put(0L, master.getRootPath());
        processorIndices.put(1L, slave.getRootPath());

        // initialize master actor
        val masterSelf = master.createActor("self");
        val masterModel = finalizeModel(PCACalculatorModel.builder()
                                                          .processorIndices(processorIndices)
                                                          .myProcessorIndex(0L)
                                                          .build(), masterSelf);
        val masterControl = new PCACalculatorControl(masterModel);
        val masterMessageInterface = createMessageInterface(masterControl);

        // initialize slave actor
        val slaveSelf = slave.createActor("self");
        val slaveModel = finalizeModel(PCACalculatorModel.builder()
                                                         .processorIndices(processorIndices)
                                                         .myProcessorIndex(1L)
                                                         .build(), slaveSelf);
        val slaveControl = new PCACalculatorControl(slaveModel);
        val slaveMessageInterface = createMessageInterface(slaveControl);

        // create projections
        val masterProjection = createMatrix(100, 5);
        val slaveProjection = createMatrix(100, 5);
        val projection = (new MatrixInitializer(masterProjection.countColumns()).append(masterProjection)
                                                                                .append(slaveProjection)
                                                                                .create());
        val projectionMin = projection.aggregateAll(Aggregator.MINIMUM).floatValue();
        val projectionMax = projection.aggregateAll(Aggregator.MAXIMUM).floatValue();

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
                                                   initializeColumnMeansTransfer,
                                                   true).getMatrix(slaveProjection.countColumns());
        assertThat(slaveColumnMeans.equals(Calculate.transposedColumnMeans(slaveProjection), MATRIX_COMPARISON_CONTEXT)).isTrue();

        // send slave R
        val slaveR = performDataTransfer(masterSelf, masterMessageInterface,
                                         slaveSelf, slaveMessageInterface,
                                         initializeRTransfer,
                                         false).getMatrix(slaveProjection.countColumns());
        assertThat(slaveR.equals(masterModel.getRemoteRsByProcessStep().get(0L), MATRIX_COMPARISON_CONTEXT)).isTrue();
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
        model.getProcessorIndices().put(0L, master.getRootPath());
        model.setMyProcessorIndex(0L);
        val control = new PCACalculatorControl(model);
        val messageInterface = createMessageInterface(control);

        val projection = createMatrix(100, 5);
        val projectionMin = projection.aggregateAll(Aggregator.MINIMUM).floatValue();
        val projectionMax = projection.aggregateAll(Aggregator.MAXIMUM).floatValue();
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
