from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations

SORT_VALUE=True

class MBA(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,reducer=self.reducer),
            MRStep(mapper=self.mapper2, combiner=self.combiner, reducer=self.reducer2),
            MRStep(reducer=self.reducer3),
        ]
    def mapper (self,_,line):
        invoiceId,ProductId,,,_ = line.split(';')
        yield invoiceId,ProductId

    def reducer (self, key, values):
        yield key, values

    def mapper2 (self, key, values):
         items2= list(values)
         items2.sort()
         combs = combinations(items2,2)
         for comb in combs:
            yield comb ,1

    def combiner(self, key, values):
        items=list(values)
        yield key, sum(items)
        
    def reducer2(self, key, values):
        items=list(values)
        yield None, (key, sum(items))

    def reducer3(self, key, values):
        items = list(values)

        for item in items:
            yield item[0], (item[1])
        
if _name_ == '_main_':
    MBA.run()