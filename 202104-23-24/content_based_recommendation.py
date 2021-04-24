from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import statistics
import sys

class ContentBasedRecommendation(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_phase1, reducer=self.reducer_phase1),
            MRStep(mapper=self.mapper_phase2, reducer=self.reducer_phase2),
            MRStep(mapper=self.mapper_phase3, reducer=self.reducer_phase3)
        ]

    def mapper_phase1(self, _, line):
        userid, movie, rating, _ = line.split('	')

        yield float(movie), (userid, float(rating))

    def reducer_phase1(self, key, values):
        movie = key
        items = list(values)
        numberOfRaters = len(items)

        for item in items:
            userId = item[0]
            rating = item[1]

            yield userId, (movie, rating, numberOfRaters) 

    def mapper_phase2(self, key, value):
        (movie, rating, numberOfRaters) = value
        userId = key

        yield userId, (movie, rating, numberOfRaters)

    def reducer_phase2(self, key, values):
        userId = key
        items = sorted(list(values))
        combs = combinations(items, 2)

        items.sort()

        for comb in combs:
            # comb => (movie1, rating1, numberOfRaters1) , (movie2, rating2, numberOfRaters2) 
            reducerKey = comb[0][0], comb[1][0]
            
            rating1 = comb[0][1]
            rating2 = comb[1][1]

            numberOfRaters1 = comb[0][2]
            numberOfRaters2 = comb[1][2]

            ratingProduct = rating1 * rating2
            rating1Square = rating1 * rating1
            rating2Square = rating2 * rating2

            #reducerValue = (rating1, numberOfRaters1, rating2, numberOfRaters2, ratingProduct, rating1Square, rating2Square)
            reducerValue = (rating1, numberOfRaters1, rating2, numberOfRaters2)

            yield reducerKey, reducerValue
        
    def mapper_phase3(self, key, value):
        movie1, movie2 = key
        rating1, numberOfRaters1, rating2, numberOfRaters2 = value
        
        yield (movie1, movie2), (rating1, numberOfRaters1, rating2, numberOfRaters2)

    def reducer_phase3(self, key, values):
        items = list(values)

        movie1, movie2 = key 

        values1 = []
        values2 = []

        for item in items:
            (rating1, numberOfRaters1, rating2, numberOfRaters2) = item

            values1.append(rating1)
            values2.append(rating2)

        min1 = min(values1)
        min2 = min(values2)
        max1 = max(values1)
        max2 = max(values2)
        avg1 = statistics.mean(values1)
        avg2 = statistics.mean(values2)
        std1 = statistics.stdev(values1)
        std2 = statistics.stdev(values2)

        feature1 = [min1, max1, avg1, std1]
        feature2 = [min2, max2, avg2, std2]

        yield (movie1, movie2), (feature1, feature2)



if __name__ == '__main__':
    ContentBasedRecommendation.run()