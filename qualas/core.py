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

        # self.data = array('L', [0] * longs_length)
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

    def __len__(self):
        return self.length


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

        print


class DataFrame(object):
    def __init__(self):
        self.columns = defaultdict(DateFrameColumn)
        self.metrics = defaultdict(DataFrameMetric)


class DataFrameLoaderFromCsv(object):
    def __init__(self, dimensions, metrics, delimiter):
        self.dimensions = dimensions
        self.metrics = metrics
        self.delimiter = delimiter

    def load(self, file_object):
        full_headers_list = file_object.readline().strip('\n').split(self.delimiter)

        data_frame = DataFrame()

        columns = self.dimensions + self.metrics
        csv_reader = RecyclingCsvReader(file_object, columns, full_headers_list, delimiter='\t')

        for record in csv_reader:
            for dimension in self.dimensions:
                data_frame.columns[dimension].insert_value(record[dimension])
            for metric in self.metrics:
                data_frame.metrics[metric].insert_value(record[metric])

        for column in data_frame.columns.values():
            column.finalize_bitmaps()

        return data_frame


class RecyclingCsvReader(object):
    def __init__(self, file_object, relevant_columns, full_headers_list, empty_value='', delimiter=','):
        self.file_object = file_object
        self.relevant_columns = relevant_columns
        self.full_headers_list = full_headers_list
        self.empty_value = empty_value
        self.delimiter = delimiter

        self.indexes = {i: self.full_headers_list[i] for i in xrange(len(self.full_headers_list))
                        if self.full_headers_list[i] in self.relevant_columns}

        self.ordered_relevant_indexes = sorted(self.indexes.keys())
        self.ordered_relevant_columns = [header for header in self.full_headers_list if header in self.relevant_columns]

    def _init_file(self):
        self.file_object.seek(0)
        self.file_object.readline()

        self.reusable_dict = dict.fromkeys(self.relevant_columns, self.empty_value)

    def get_fast_csv_record(self, line):
        split_line = line.strip('\n').split(self.delimiter)
        self.reusable_dict.update(
            ((self.indexes[i], split_line[i] if self.indexes[i] != '' else self.empty_value) for i in self.indexes)
        )
        return self.reusable_dict

    def iterator(self):
        for line in self.file_object:
            yield self.get_fast_csv_record(line)

    def __iter__(self):
        self._init_file()
        return self.iterator()
