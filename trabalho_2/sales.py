from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter

class Sales(MRJob):
    def mapper(self, _, line):
        sale = line.split(',')
        product = sale[1]
        country = sale[7]

        yield (country, product), 1

    def reducer(self, key, values):
        items = list(values)
        yield key[0], (key[1], sum(items))

    def reducer_2(self, key, values):
        items = list(values)

        yield key, items
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer_2)
        ]


if __name__ == '__main__':
    Sales.run()