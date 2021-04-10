from mrjob.job import MRJob 
from mrjob.step import MRStep


class TopN(MRJob):
    top = []
    N = 5

    def mapper(self, _, line):
        weight, catId, name = line.split(',')

        weight = float(weight)
        self.top.append((weight, name))

        if len(self.top) > self.N:
            self.top.sort()
            self.top.pop(0)

    def reducer_init(self):
        for item in self.top:
            yield None, item

    def reducer(self, key, values):
        items = sorted(list(values), reverse=True)

        for i in range(self.N):
            item = items[i]
            weight = item[0]
            name = item[1]

            yield name, weight
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer_init = self.reducer_init),
            MRStep(reducer=self.reducer)
        ]


if __name__ == '__main__':
    TopN.run()