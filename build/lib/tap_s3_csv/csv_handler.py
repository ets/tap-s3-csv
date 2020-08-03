import csv
import re

from tap_s3_csv.logger import LOGGER as logger

class CustomDictReader(csv.DictReader):

    def __next__(self):
        if self.line_num == 0:
            # Used only for its side effect.
            self.fieldnames
        row = next(self.reader)
        self.line_num = self.reader.line_num

        # unlike the basic reader, we prefer not to return blanks,
        # because we will typically wind up with a dict full of None
        # values
        while row == []:
            row = next(self.reader)

        #TODO: if config['strip_newlines']:                
        d = dict(zip(self.fieldnames, [ re.sub(r'\u000D\u000A|[\u000A\u000B\u000C\u000D\u0085\u2028\u2029]', '', item) for item in row ] ))
        # d = dict(zip(self.fieldnames, row ))

        lf = len(self.fieldnames)
        lr = len(row)
        if lf < lr:
            d[self.restkey] = row[lf:]
        elif lf > lr:
            for key in self.fieldnames[lr:]:
                d[key] = self.restval
        return d


def generator_wrapper(reader):
    to_return = {}

    for row in reader:
        for key, value in row.items():
            if key is None:
                key = '_s3_extra'

            formatted_key = key

            # remove non-word, non-whitespace characters
            formatted_key = re.sub(r"[^\w\s]", '', formatted_key)

            # replace whitespace with underscores
            formatted_key = re.sub(r"\s+", '_', formatted_key)
            to_return[formatted_key.lower()] = value

        yield to_return


def get_row_iterator(table_spec, file_handle):
    field_names = None

    if 'field_names' in table_spec:
        field_names = table_spec['field_names']

    reader = CustomDictReader(file_handle, fieldnames=field_names)

    return generator_wrapper(reader)
