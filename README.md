
# Assignment 1: Single Stream, Operation on elements

The goal of this assignment is to process and convert historical 3-month T-Bill yields taken from the US treasury department with prices. This result will then be used as a basis for the subsequent assignments. 

### Task

* Read in the data as a stream, convert daily closing yield to price and output to the console. 
* Create a simple unit test to check the output. 

### Input Data

* For this assignment you will work with a single data stream.
* It is a CSV file of the daily high, low, open, closing yields for the 3-month T-bill from 2000 to present

The `TBillRateSource` annotates the generated `DataStream<TaxiFare>` with timestamps and watermarks. Hence, there is no need to provide a custom timestamp and watermark assigner in order to correctly use event time.

### Expected Output

The result of this exercise is a data stream of Tuple3<Date, Double, Double> or (Date, Yield, Price)

The resulting stream should be printed to standard out.

## Assumptions

* The data used for input is an example of bounded (finite) stream and would normally be processed as a DataSet. However, we will treat the data as an unordered stream as in the the Taxi rides and fares exercises. We will modify the existing TaxiFareSource class to support our new dataset of bond yields. This source will emulate an unordered, unbounded stream. 
* T-bills prices are discounted from par (assumed to be $100) using a simple interest formula. 
* The price can be calculated as: P = 100 -  r * t where r is the yield in decimal and t is 91/360 years (3 months in the 30/360 day count convention)

### Exercise Classes

- Java:  [`org.apache.flink.training.exercises.hourlytips.TbillPriceAssignment`](src/main/java/org/apache/flink/training/exercises/hourlytips/HourlyTipsExercise.java)

### Tests

- Java:  [`org.apache.flink.training.exercises.hourlytips.TbillPriceAssignment`](src/test/java/org/apache/flink/training/exercises/hourlytips/TBillPriceAssignmentTest.java)

## Implementation Hints

<details>
<summary><strong>Program Structure</strong></summary>

Note that it is possible to cascade one set of time windows after another, so long as the timeframes are compatible (the second set of windows needs to have a duration that is a multiple of the first set). So you can have a initial set of hour-long windows that is keyed by the `driverId` and use this to create a stream of `(endOfHourTimestamp, driverId, totalTips)`, and then follow this with another hour-long window (this window is not keyed) that finds the record from the first window with the maximum `totalTips`.
</details>

## Documentation

- [Windows](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html)
- [See the section on aggregations on windows](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/#datastream-transformations)

## Reference Solutions

Reference solutions are available in the projects:

- Java:  [`org.apache.flink.training.assignments.tbillprices.HourlyTipsSolution`](src/java/org/apache/flink/training/assignments/tbillprices/TBillPriceAssignment.java)
-----
