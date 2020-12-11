# qualas

An efficient (as possible) pure Python library for data analysis

Currently, it's only a little milestone, but it can be continued to fill some features and tools to complete the  idea 
of Python purity and efficiency

## how to install

    pip install qualas

## What does it contain

### RecyclingCsvReader

An experiment to read csv-s faster in pure python

```python
from qualas.core import RecyclingCsvReader

csv_file_object = open('data.csv', 'r')
reader = RecyclingCsvReader(
    csv_file_object, 
    relevant_columns=['A', 'B'],
    full_headers_list=['A', 'B', 'C']
)
for row in reader:
    print(row['A'], row['B'])
```

### DataFrameLoaderFromCsv

A tool to read csv-s into columnar data-frame

```python
from qualas.core import DataFrameLoaderFromCsv

csv_file_object = open('data.csv', 'r')
data_frame = DataFrameLoaderFromCsv(
    dimensions=['A'], 
    metrics=['B']
).load(
    csv_file_object
)
```

### DataFrame

A collection of dimension and metrics to use for fast analytics

```python
from qualas.core import DataFrame, DateFrameColumn, DataFrameMetric

a = DateFrameColumn()
a.insert_value('val1')
a.insert_value('val2')
a.insert_value('val1')
a.finalize_bitmaps()

b = DataFrameMetric()
b.insert_value(11)
b.insert_value(12)
b.insert_value(13)

data_frame = DataFrame()
data_frame.columns['A'] = a
data_frame.metrics['B'] = b
```

### BitArrayStream

A stream to create BitArray

```python
from qualas.core import BitArrayStream
stream = BitArrayStream(3)
stream.add_value(True)
stream.add_value(False)
stream.add_value(False)

bit_array = stream.finalize()
```

### BitArray

A pure python bit arrays using python array

```python
from qualas.core import BitArray

bitarray = BitArray(12)
bitarray[0] = True
bitarray[8] = True
```
