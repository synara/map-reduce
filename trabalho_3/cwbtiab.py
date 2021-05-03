from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from operator import itemgetter, attrgetter
import collections



class CWBTIAB(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_phase1, reducer=self.reducer_phase1),
            MRStep(mapper=self.mapper_phase2, reducer=self.reducer_phase2),
        ]

    def mapper_phase1(self, _, line):
        invoiceno, stockcode, quantity, customerid, country = line.split(';')

        yield (customerid), stockcode

    def reducer_phase1(self, key, values):
        items = list(values)
        yield key, items

    def mapper_phase2(self, key, value):
        items = value
        items.sort()

        for i in range(len(items)):
            item1 = items[i]

            m = {}
            for j in range(i+1, len(items)):
                item2 = items[j]

                if item2 not in m:
                    m[item2] = 0

                m[item2] = m[item2] + 1

            yield item1, m

    def reducer_phase2(self, key, values):
        stripes = list(values)

        final = {}

        for m in stripes:
            for k, v in m.items():
                if k not in final:
                    final[k] = 0
                final[k] = final[k] + v


        sorted_dict = collections.OrderedDict(sorted(final.items(), key=itemgetter(1), reverse=True))
        top5 = list(sorted_dict.items())[:5]
        yield key, top5


if __name__ == '__main__':
    CWBTIAB.run()
