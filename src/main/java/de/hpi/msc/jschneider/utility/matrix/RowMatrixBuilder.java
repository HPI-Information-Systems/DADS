package de.hpi.msc.jschneider.utility.matrix;

import lombok.val;
import org.ojalgo.array.Primitive64Array;
import org.ojalgo.matrix.Primitive64Matrix;
import org.ojalgo.matrix.store.MatrixStore;
import org.ojalgo.structure.Access2D;

import java.util.ArrayList;
import java.util.List;

public class RowMatrixBuilder implements MatrixBuilder
{
    private final List<Primitive64Array> rows = new ArrayList<>();
    private final long columns;

    public static MatrixStore<Double> concat(Access2D<Double> first, Access2D<Double> second)
    {
        assert first.countColumns() == second.countColumns() : "First and second matrix must have same amount of columns for concat!";

        return (new RowMatrixBuilder(first.countColumns()))
                .append(first)
                .append(second)
                .build();
    }

    public RowMatrixBuilder(long columns)
    {
        this.columns = columns;
    }

    public RowMatrixBuilder append(double[] row)
    {
        assert row.length == columns : "Row must have the same amount of columns for append!";

        rows.add(Primitive64Array.wrap(row));
        return this;
    }

    public RowMatrixBuilder append(Access2D<Double> matrix)
    {
        assert matrix.countColumns() == columns : "Matrix must have the same amount of columns for append!";

        for (val row : matrix.rows())
        {
            append(row.toRawCopy1D());
        }

        return this;
    }

    public MatrixStore<Double> build()
    {
        val result = Primitive64Matrix.FACTORY.rows(rows.toArray(new Primitive64Array[0]));
        rows.clear();

        return MatrixStore.PRIMITIVE64.makeWrapper(result).get();
    }
}
