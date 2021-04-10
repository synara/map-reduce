from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class Movies(MRJob):
    def mapper_movies(self, _, line):
        user_id, movie_id, rating, timestamp = line.split()
        yield movie_id, float(rating)
    
    def reducer_one(self, key, values):
        ratings = list(values)
        yield  sum(ratings)/len(ratings), key

    def reducer_two(self, key, values):
        items = sorted(list(values))
        
        for i in items:
            yield int(i), key


    def steps(self):
        return [
            MRStep(mapper=self.mapper_movies,
                   reducer=self.reducer_one),
            MRStep(reducer=self.reducer_two)
        ]

if __name__ == '__main__':
    Movies.run()
