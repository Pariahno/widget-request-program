import unittest
from unittest.mock import patch, Mock
from consumer import(
    read_requests,
    process_request,
    store_widget_in_bucket,
    store_widget_in_table,
    get_bucket_list,
    get_table_list,
    )


class TestConsumer(unittest.TestCase):
    
    @patch('boto3.client')
    def test_read_requests(self, mock_client):
        mock_s3_client = mock_client('s3')
        


if __name__ == '__main__':
    unittest.main()
    