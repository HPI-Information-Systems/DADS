# Distributed Detection of Sequential Anomalies in Univariate Time Series

Source code for the Distributed Anomaly Detection System (DADS)

This algorithm is based on the [Series2Graph](http://helios.mi.parisdescartes.fr/~themisp/series2graph/) algorithm, that was published in `P. Boniol and T. Palpanas, Series2Graph: Graph-based Subsequence Anomaly Detection in Time Series, PVLDB (2020)`.

## Usage

### Building

Requirements:

- JDK 8 (Java)
- Maven

```sh
mvn package
```

You can skip the tests with `-DskipTests`.
The created fat JAR is located in the `target`-folder.

### Running

Requirements:

- JRE 8 (Java)

```sh
java -jar <dads-file-name>.jar master --host localhost --port 7788 --min-slaves 0 \
    --sequence <input_file> \
    --sub-sequence-length 50 --intersection-segments 50 \
    --query-length 75 --convolution-size 16 \
    --output ./results.txt --no-statistics
```
