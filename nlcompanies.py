from mrjob.job import MRJob
from mrjob.step import MRStep

class CountDutchCompanies(MRJob):
    
    def mapper(self, _, line):
        # Skip the first line, which contains the variable names
        if line.startswith("Country"):
            return
        # split the line by comma and get the first field
        fields = line.split(",")
        country = fields[0]
        
        # if the country is "Netherlands" the value is 1 and as a key i just did none because I dont need that.
        if country == "Netherlands":
            yield None, 1
    
    # reduce function where i use _ to ignore the key and "counts" to represent that values
    def reducer(self, _, counts):
        yield "Number of AI companies in the Netherlands:", sum(counts)

if __name__ == '__main__':
    CountDutchCompanies.run()
