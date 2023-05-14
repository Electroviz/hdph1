from mrjob.job import MRJob
from mrjob.step import MRStep

class CountAICompaniesPerCountry(MRJob):
    
    def mapper(self, _, line):
        # Skip the first line because there are the variable names
        if line.startswith("Country"):
            return
        # Set the seperator for this dataset
        fields = line.split(",")
        # Extracts the first field from the line
        country = fields[0]
        # Outputs a key-value pair where the country name is the key and the value is 1
        yield country, 1
    
    # reducer function that takes a country name and a list of counts for that country
    def reducer(self, country, counts):
        # Outputs a key-value pair where the country name is the key and the value is the sum of the counts for that country
        yield country, sum(counts)

if __name__ == '__main__':
    CountAICompaniesPerCountry.run()
