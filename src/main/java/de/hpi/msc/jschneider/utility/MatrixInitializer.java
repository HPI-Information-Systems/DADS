package de.hpi.msc.jschneider.utility;

import lombok.val;
import org.ojalgo.array.Primitive64Array;
import org.ojalgo.matrix.PrimitiveMatrix;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.structure.Access2D;

import java.util.ArrayList;
import java.util.List;

public class MatrixInitializer
{
    private final List<Primitive64Array> rows = new ArrayList<>();
    private final long columns;

    public static MatrixStore<Double> concat(Access2D<Double> first, Access2D<Double> second)
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

    public MatrixInitializer appendRow(double[] row)
    {
        assert row.length == columns : "Row must have the same amount of columns for append!";

        rows.add(Primitive64Array.wrap(row));
        return this;
    }

    public MatrixInitializer append(Access2D<Double> matrix)
    {
        assert matrix.countColumns() == columns : "Matrix must have the same amount of columns for append!";

        for (val row : matrix.rows())
        {
            appendRow(row.toRawCopy1D());
        }

        return this;
    }

    public MatrixStore<Double> create()
    {
        val result = PrimitiveMatrix.FACTORY.rows(rows.toArray(new Primitive64Array[0]));
        rows.clear();

        return MatrixStore.PRIMITIVE.makeWrapper(result).get();
    }
}
