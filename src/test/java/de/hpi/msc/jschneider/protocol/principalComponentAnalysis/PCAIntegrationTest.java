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
import org.ojalgo.matrix.decomposition.Eigenvalue;
import org.ojalgo.matrix.decomposition.SingularValue;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.matrix.store.PrimitiveDenseStore;
import org.ojalgo.type.context.NumberContext;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class PCAIntegrationTest extends BaseTestCase
{
    private static final int PRECISION = 5;
    private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;
    private static final NumberContext MATRIX_COMPARISON_CONTEXT = NumberContext.getMath(new MathContext(PRECISION, ROUNDING_MODE));

    @Override
    protected ProtocolType[] getProcessorProtocols()
    {
        return new ProtocolType[]{ProtocolType.MessageExchange, ProtocolType.PrincipalComponentAnalysis};
    }

    private MatrixStore<Double> createMatrix()
    {
        val columns = 5L;
        val rows = 100L;

        val matrixInitializer = new MatrixInitializer(columns);
        val random = new Random();

        for (var rowIndex = 0L; rowIndex < rows; ++rowIndex)
        {
            val row = new float[(int) columns];
            for (var columnIndex = 0; columnIndex < columns; ++columnIndex)
            {
                val bigDecimal = new BigDecimal(Float.toString(random.nextFloat())).setScale(PRECISION, ROUNDING_MODE);
                row[columnIndex] = bigDecimal.floatValue();
            }
            matrixInitializer.appendRow(row);
        }

        return matrixInitializer.create();
    }

    private SequenceSliceDistributionEvents.ProjectionCreatedEvent sendProjectionCreatedEvent(TestProbe receiver, PartialFunction<Object, BoxedUnit> messageInterface, MatrixStore<Double> projection)
    {
        val event = SequenceSliceDistributionEvents.ProjectionCreatedEvent.builder()
                                                                          .receiver(receiver.ref())
                                                                          .sender(receiver.ref())
                                                                          .firstSubSequenceIndex(0L)
                                                                          .projection(projection)
                                                                          .build();
        messageInterface.apply(event);

        return event;
    }

    private void comparePCA(SingularValue<Double> svd, Eigenvalue<Double> evd)
    {
        assertThat(svd.getV().countColumns()).isEqualTo(evd.getV().countColumns());
        assertThat(svd.getV().countRows()).isEqualTo(evd.getV().countRows());

        for (var i = 0; i < svd.getV().countColumns(); ++i)
        {
            val svdColumn = svd.getV().logical().column(i).get();
            val evdColumn = evd.getV().logical().column(i).get();

            val isEqual =
                    svdColumn.equals(evdColumn, MATRIX_COMPARISON_CONTEXT) ||
                    svdColumn.equals(evdColumn.multiply(-1.0d), MATRIX_COMPARISON_CONTEXT);

            assertThat(isEqual).isTrue();
        }
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

        val projection = createMatrix();
        val covariances = DataProcessors.covariances(PrimitiveDenseStore.FACTORY, projection);
        val evd = Eigenvalue.PRIMITIVE.make(covariances);
        evd.decompose(covariances);

        val projectionCreatedEvent = sendProjectionCreatedEvent(self, messageInterface, projection);
        val svdEvent = expectEvent(PCAEvents.SingularValueDecompositionCreatedEvent.class, master);

        comparePCA(svdEvent.getSvd(), evd);

        assertThatMessageIsCompleted(projectionCreatedEvent, master);
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
        val masterProjection = createMatrix();
        val slaveProjection = createMatrix();
        val projection = (new MatrixInitializer(masterProjection.countColumns()).append(masterProjection)
                                                                                .append(slaveProjection)
                                                                                .create());

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

        // expect master to finish calculation
        val svdEvent = expectEvent(PCAEvents.SingularValueDecompositionCreatedEvent.class, master);

        comparePCA(svdEvent.getSvd(), evd);
    }
}
