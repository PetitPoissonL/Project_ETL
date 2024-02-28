import unittest
from src.extract import get_taxon_key, extract_data


class TestExtract(unittest.TestCase):

    def test_get_taxon_key(self):
        # Tests the taxon_key retrieval function
        scientific_name = 'Panthera leo'
        taxon_key = get_taxon_key(scientific_name)
        self.assertIsNotNone(taxon_key)

    def test_extract_data_valid_taxon_key(self):
        # Tests data extraction when the taxon_key is valid
        path = '/Users/ningyu/Desktop/Projets_data/ETL_Panthera_leo'
        taxon_key = '5219404'
        data = extract_data(path, taxon_key)
        self.assertIsNotNone(data)


if __name__ == '__main__':
    unittest.main()
