package de.hpi.msc.jschneider.math.kernelDensity;

import de.hpi.msc.jschneider.math.Calculate;
import junit.framework.TestCase;
import lombok.val;
import lombok.var;

import static org.assertj.core.api.Assertions.assertThat;

public class TestGaussianKernelDensity extends TestCase
{
    public void testEstimate1()
    {
        val samples = new double[]{0.0d, 0.125d, 0.25d, 0.33d, 0.34d, 0.35d, 0.75d, 1.0d};
        val h = Calculate.scottsFactor(samples.length, 1L);
        val kde = new GaussianKernelDensity(samples, h);

        val probabilitySamples = new double[]{0.0d, 1.0d, 2.0d, 3.0d, 4.0d, 5.0d, 6.0d, 7.0d, 8.0d, 9.0d};
        val probabilities = kde.evaluate(probabilitySamples);

        val expectedValues = new double[] // values determined by executing scipy.stats.gaussian_kde with the same samples
                {
                        0.7457262128225104,
                        0.3561172153996687,
                        5.196676229610921e-06,
                        5.880407794084188e-20,
                        3.367331176077681e-43,
                        9.734698687911735e-76,
                        1.4207333137391282e-117,
                        1.0467813083172548e-168,
                        3.893612242617612e-229,
                        7.31143768705744e-299
                };

        for (var i = 0; i < probabilitySamples.length; ++i)
        {
            assertThat(Calculate.isSame(probabilities[i], expectedValues[i])).isTrue();
        }
    }
}
