from mrjob.job import MRJob
from mrjob.step import MRStep

class Top10AICountries(MRJob):
    
    def mapper(self, _, line):
        # skip the first line
        if line.startswith("Country"):
            return
        # Define the seperator for the csv file
        fields = line.strip().split(",")
        country = fields[0]
        # Outputs a key-value pair where the country name is the key and the value is 1
        yield country, 1
    
    # combiner function to aggregate the counts by country
    def combiner(self, country, counts):
        yield country, sum(counts)
    
    # reducer function to aggregate the counts by country from the mapper outputs and generate a single key-value 
    def reducer(self, country, counts):
        yield None, (sum(counts), country)
    
    # second reducer to generate the top 10 countries with the gighest number of AI companies. 
    def reducer_top10(self, _, counts):
        # Loop trough the sorted list of key-value paris, starting with the highest count.
        for count, country in sorted(counts, reverse=True)[:10]:
            yield country, count
    
    # Defina a MRStep object to specify the order of the steps
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_top10)
        ]

if __name__ == '__main__':
    Top10AICountries.run()
