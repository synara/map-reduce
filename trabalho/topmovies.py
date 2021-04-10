from mrjob.job import MRJob 
from mrjob.step import MRStep

class TopMovies(MRJob):
    def mapper(self, _, line):
        user_id, movie_id, rating, timestamp = line.split()

        yield movie_id, float(rating)

    def reducer(self, key, values):
        items = list(values)
        yield None, (sum(items)/len(items), key)

    def reducer_2(self, key, values):
        top10 = sorted(list(values), reverse=True)[:10]

        for i in range(len(top10)):
            item = top10[i]
            movie = item[1]
            avg = item[0]

            yield movie, avg
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer_2)
        ]


if __name__ == '__main__':
    TopMovies.run()