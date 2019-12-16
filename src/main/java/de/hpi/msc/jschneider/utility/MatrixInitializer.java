package de.hpi.msc.jschneider.utility;

import de.hpi.msc.jschneider.utility.dataTransfer.DataSink;
import lombok.val;
import lombok.var;
import org.ojalgo.array.Primitive32Array;
import org.ojalgo.matrix.PrimitiveMatrix;
import org.ojalgo.structure.Access2D;

import java.util.ArrayList;
import java.util.List;

public class MatrixInitializer implements DataSink
{
    private final List<Primitive32Array> rows = new ArrayList<>();
    private final long columns;

    public static PrimitiveMatrix concat(Access2D<Double> first, Access2D<Double> second)
    {
        assert first.countColumns() == second.countColumns() : "First and second matrix must have same amount of columns for concat!";

        return (new MatrixInitializer(first.countColumns()))
                .append(first)
                .append(second)
                .create();
    }

    public MatrixInitializer(long columns)
    {
        this.columns = columns;
    }

    public MatrixInitializer appendRow(float[] row)
    {
        assert row.length == columns : "Row must have the same amount of columns for append!";

        rows.add(Primitive32Array.wrap(row));
        return this;
    }

    public MatrixInitializer append(Access2D<Double> matrix)
    {
        assert matrix.countColumns() == columns : "Matrix must have the same amount of columns for append!";

        for (val row : matrix.rows())
        {
            val floats = new float[(int) row.count()];
            for (var floatsIndex = 0; floatsIndex < floats.length; ++floatsIndex)
            {
                floats[floatsIndex] = row.get(floatsIndex).floatValue();
            }

            appendRow(floats);
        }

        return this;
    }

    public PrimitiveMatrix create()
    {
        val result = PrimitiveMatrix.FACTORY.rows(rows.toArray(new Primitive32Array[0]));
        rows.clear();

        return result;
    }

    @Override
    public void write(float[] part)
    {
        appendRow(part);
    }

    @Override
    public void close()
    {
    }
}
