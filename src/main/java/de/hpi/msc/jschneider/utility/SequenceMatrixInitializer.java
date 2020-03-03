package de.hpi.msc.jschneider.utility;

import com.google.common.primitives.Doubles;
import de.hpi.msc.jschneider.math.SequenceMatrix;
import lombok.val;
import lombok.var;
import org.ojalgo.matrix.store.MatrixStore;

import java.util.ArrayList;
import java.util.List;

public class SequenceMatrixInitializer
{
    private final int sequenceLength;
    private final int convolutionSize;
    private final List<Double> data = new ArrayList<>();
    private final List<Double> unusedData = new ArrayList<>();
    private double convolutionSum = 0.0d;

    public SequenceMatrixInitializer(int sequenceLength, int convolutionSize)
    {
        this.sequenceLength = sequenceLength;
        this.convolutionSize = convolutionSize;
    }

    public SequenceMatrixInitializer append(double[] dataPart)
    {
        unusedData.addAll(Doubles.asList(dataPart));
        if (isFirstDataPart() && unusedData.size() < convolutionSize)
        {
            return this;
        }

        if (isFirstDataPart())
        {
            for (var convolutionIndex = 0; convolutionIndex < convolutionSize; ++convolutionIndex)
            {
                convolutionSum += unusedData.get(convolutionIndex);
            }
            data.add(convolutionSum);
            convolutionSum -= unusedData.remove(0);
        }

        while (unusedData.size() >= convolutionSize && unusedData.size() >= sequenceLength)
        {
            convolutionSum += unusedData.get(convolutionSize - 1);
            data.add(convolutionSum);

            convolutionSum -= unusedData.remove(0);
        }

        return this;
    }

    private boolean isFirstDataPart()
    {
        return data.isEmpty();
    }

    public MatrixStore<Double> create()
    {
        val matrix = new SequenceMatrix(sequenceLength - convolutionSize, Doubles.toArray(data));
        data.clear();
        unusedData.clear();

        return matrix;
    }
}
