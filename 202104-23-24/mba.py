from mrjob.job import MRJob 
from itertools import combinations

class MBA(MRJob):

    def get_combinations(self, items):
        result = []

        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                for k in range(j + 1, len(items)):
                    a = items[i]
                    b = items[j]
                    c = items[k]

                    result.append((a, b, c))
    
        return result


    def mapper(self, _, line):
        items = sorted(line.split(','))

        combs = self.get_combinations(items) #combinations(items, 2)

        for comb in combs:
            yield comb, 1

    def reducer(self, key, values):
        items = list(values)

        yield key, sum(items)

    def combiner(self, key, values):
        items = list(values)

        yield key, sum(items)  

    

if __name__ == "__main__":
    MBA.run()

    # items = ['p', 'b', 'a', 'd']
    # items = sorted(items)
    # mba = MBA()

    # print(mba.get_combinations(items))