from mrjob.job import MRJob
from mrjob.step import MRStep

class MarvelGraph(MRJob):
    def mapper(self, key, line):
        heroes = line.split(' ')
        
        _id = heroes[0]
        _friends = len(heroes) - 2

        yield _id, int(_friends)

    def reducer(self, key, values):
        items = list(values)

        yield None, (sum(items), key)

    def reducer_2(self, key, values):
        friends = sorted(list(values), reverse = True)
        first = friends[0]

        yield int(first[1]), first[0]
        
    
    def steps(self):
        return [
                MRStep(mapper=self.mapper,
                    reducer=self.reducer),
                MRStep(reducer=self.reducer_2)
            ]


if __name__ == '__main__':
    MarvelGraph.run()