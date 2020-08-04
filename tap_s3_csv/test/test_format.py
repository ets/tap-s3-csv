import unittest
import re
import smart_open
import codecs


from tap_s3_csv.format_handler import get_filetype_handler,get_streamreader,monkey_patch_streamreader
import tap_s3_csv.conversion as conversion

TEST_TABLE_SPEC = {
    "name": "products",
    "pattern": "g2/.*roduct.*",
    "key_properties": ["id"],
    "format": "csv",
    "schema_overrides": {
        "id": {
            "type": "integer",
            "_conversion_type": "integer"
        }
    }
}


class TestFormatHandler(unittest.TestCase):

    def test_strip_newlines_local_custom(self):
        test_filename = './tap_s3_csv/test/sample_with_bad_newlines.csv'
                
        file_handle = smart_open.open(test_filename, 'rb', errors='surrogateescape')
        reader = codecs.getreader('utf-8')(file_handle)
        reader = monkey_patch_streamreader(reader)
        iterator = get_filetype_handler(TEST_TABLE_SPEC, reader)
                
        for row in iterator:                        
            self.assertTrue( row['id'].isnumeric(), "Parsed ID is not a number for: {}".format(row['id']) )
                

   
   
