import unittest
import re

from tap_s3_csv.format_handler import get_filetype_handler,open_file_for_streaming,get_file_handle
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

    # def test_strip_newlines_remote(self):
    #     bucket = "int-pl-anc-data-lake"
    #     s3_path = "g2/2020/G2_Products_2020-04.csv"
    #     test_filename = f"s3://{bucket}/"+s3_path
        
    #     file_handle = open_file_for_streaming(test_filename, 'r', errors='surrogateescape')
    #     # file_handle = get_file_handle({'bucket':bucket},s3_path)
    #     iterator = get_filetype_handler(TEST_TABLE_SPEC, file_handle)
        
    #     for row in iterator:                                   
    #         self.assertTrue( row['id'].isnumeric(), "Parsed ID is not a number for: {}".format(row['id']) )

    def test_strip_newlines_local_original(self):
        test_filename = './tap_s3_csv/test/G2_Products_2020-04.csv'
        
        file_handle = open_file_for_streaming(test_filename, 'r', errors='surrogateescape')
        iterator = get_filetype_handler(TEST_TABLE_SPEC, file_handle)
        
        for row in iterator:                        
            self.assertTrue( row['id'].isnumeric(), "Parsed ID is not a number for: {}".format(row['id']) )
            # if 'description' in row and row['description']:
            #     self.assertFalse( re.match(r'\u000D\u000A|[\u000A\u000B\u000C\u000D\u0085\u2028\u2029]',row['description']), "Unexpected newline character found in description: {}".format(row['description']) )                

    def test_strip_newlines_local_custom(self):
        test_filename = './tap_s3_csv/test/sample_with_bad_newlines.csv'
        
        file_handle = open_file_for_streaming(test_filename, 'r', errors='surrogateescape')
        iterator = get_filetype_handler(TEST_TABLE_SPEC, file_handle)
                
        for row in iterator:                        
            self.assertTrue( row['id'].isnumeric(), "Parsed ID is not a number for: {}".format(row['id']) )
            # if 'description' in row and row['description']:                
            #     self.assertFalse( re.match(r'\u000D\u000A|[\u000A\u000B\u000C\u000D\u0085\u2028\u2029]',row['description']), "Unexpected newline character found in description: {}".format(row['description']) )
                

   
   
