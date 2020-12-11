from array import array
from collections import defaultdict


class BitArray(object):
    LONG_BYTE_LENGTH = array('L').itemsize
    LONG_BIT_LENGTH = 8 * LONG_BYTE_LENGTH
    ELEMENT_INDEX_MASK = LONG_BIT_LENGTH - 1

    INIT_STRING = '\x00' * LONG_BYTE_LENGTH

    def __init__(self, length):
        self.length = length

        longs_length = (length / self.LONG_BIT_LENGTH) + 1
        longs_length += 1 if length % self.LONG_BIT_LENGTH == 0 else 0

        self.data = array('L', self.INIT_STRING * longs_length)

    def __setitem__(self, index, value):
        if index > self.length:
            raise IndexError()

        higher_index = index >> self.LONG_BIT_LENGTH
        lower_index = index & self.ELEMENT_INDEX_MASK

        if value:
            mask = 1 << lower_index
            self.data[higher_index] |= mask
        else:
            mask = ~ (1 << lower_index)
            self.data[higher_index] &= mask

    def set_byte(self, byte_index, byte_value):
        self.data[byte_index] = byte_value

    def __len__(self):
        return self.length


class BitArrayStream(object):
    def __init__(self, length):
        self.bit_array = BitArray(length)

        self.array_index = 0
        self.byte_index = 8
        self.byte_value = 0

    def add_value(self, value):
        self.byte_index -= 1
        self.byte_value |= (value << self.byte_index)

        if self.byte_index == 0:
            self.bit_array.set_byte(self.array_index, self.byte_value)
            self.array_index += 1
            self.byte_index = 8

    def finalize(self):
        if self.byte_index != 0:
            self.bit_array.set_byte(self.array_index, self.byte_value)
        return self.bit_array


class DataFrameMetric(object):
    def __init__(self):
        self.values = []

    def insert_value(self, value):
        self.values.append(value)


class DateFrameColumn(object):
    def __init__(self):
        self.next_index = 0

        self.values_indexes = {}
        self.column_data = []
        self.bitmaps = {}

    def _get_value_index(self, value):
        if value not in self.values_indexes:
            self.values_indexes[value] = self.next_index
            self.next_index += 1
        return self.values_indexes[value]

    def insert_value(self, value):
        current_index = self._get_value_index(value)

        self.values_indexes[value] = current_index

        self.column_data.append(current_index)

    def finalize_bitmaps(self):
        bitmaps_by_index = {}

        for index in self.values_indexes.values():
            bitmaps_by_index[index] = BitArray(len(self.column_data))

        for i, index in enumerate(self.column_data):
            bitmaps_by_index[index][i] = True

        for value, index in self.values_indexes.iteritems():
            self.bitmaps[value] = bitmaps_by_index[index]


class DataFrame(object):
    def __init__(self):
        self.columns = defaultdict(DateFrameColumn)
        self.metrics = defaultdict(DataFrameMetric)


class DataFrameLoaderFromCsv(object):
    def __init__(self, dimensions, metrics, delimiter='\t'):
        self.dimensions = dimensions
        self.metrics = metrics
        self.delimiter = delimiter

    def load(self, file_object):
        full_headers_list = file_object.readline().strip('\n').split(self.delimiter)

        data_frame = DataFrame()

        columns = self.dimensions + self.metrics
        csv_reader = RecyclingCsvReader(file_object, columns, full_headers_list, delimiter=self.delimiter)

        dimension_to_index = None
        metrics_to_index = None

        for record in csv_reader:
            dimension_to_index = dimension_to_index or {
                dimension: record.keys_to_indexes[dimension] for dimension in self.dimensions
            }
            metrics_to_index = metrics_to_index or {
                metric: record.keys_to_indexes[metric] for metric in self.metrics
            }

            for dimension, index in dimension_to_index.iteritems():
                data_frame.columns[dimension].insert_value(record.values[index])
            for metric, index in metrics_to_index.iteritems():
                data_frame.metrics[metric].insert_value(record.values[index])

        for column in data_frame.columns.values():
            column.finalize_bitmaps()

        return data_frame


class ListBasedDictionary(object):
    def __init__(self, keys):
        self.keys = keys
        self.keys_to_indexes = {value: index for index, value in enumerate(self.keys)}

        self.values = None

    def __getitem__(self, item):
        return self.values[self.keys_to_indexes[item]]


class RecyclingCsvReader(object):
    def __init__(self, file_object, relevant_columns, full_headers_list, empty_value='', delimiter=','):
        self.file_object = file_object
        self.relevant_columns = relevant_columns
        self.full_headers_list = full_headers_list
        self.empty_value = empty_value
        self.delimiter = delimiter

    def _init_file(self):
        self.file_object.seek(0)
        self.file_object.readline()

        self.reusable_dict = ListBasedDictionary(self.full_headers_list)

    def iterator(self):
        for line in self.file_object:
            self.reusable_dict.values = tuple(line.strip('\n').split(self.delimiter))
            yield self.reusable_dict

    def __iter__(self):
        self._init_file()
        return self.iterator()
