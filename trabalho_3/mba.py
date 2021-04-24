from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations


class MBA(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper=self.mapper_phase2, combiner = self.combiner, reducer = self.reducer_phase2),
            MRStep(mapper=self.mapper_phase3, reducer=self.reducer_phase3)
        ]

    def mapper(self, _, line):
        invoiceno, stockcode, quantity, customerid, country = line.split(';')

        #foram encontrados mais de um país com o mesmo código de nota.
        #com isso, foi-se considerado como chave o número da nota e país.
        yield (invoiceno, country), stockcode

    def reducer(self, key, values):
        items = list(values)
        items.sort()

        yield key, items

    def mapper_phase2(self, key, value):
        items = list(value)
        items.sort()
        combs = combinations(items, 2)

        for comb in combs:
            yield comb, 1

    def combiner(self, key, values):
        yield key, sum(list(values))

    def reducer_phase2(self, key, values):
        yield key, sum(list(values))

    def mapper_phase3(self,key,values):
        yield key[0], (key[1], values)

    def reducer_phase3(self, key, values):  
        items = sorted(list(values), key=lambda x: x[1], reverse=True)

        yield key, items

if __name__ == "__main__":
    MBA.run()