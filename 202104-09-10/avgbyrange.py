from mrjob.job import MRJob
from mrjob.step import MRStep


class AverageByRange(MRJob):
    def mapper(self, _, line):
        userId, name, age, numberOfFriends = line.split(',')

        age = int(age)
        category = None
        if age in range(18, 26):
            category = '18-25'
        elif age in range(26, 36):
            category = '26-35'
        elif age in range(36, 56):
            category = '36-55'
        elif age in range(56, 70):
            category = '56-70'

        yield category, (int(numberOfFriends), userId) # (numberOfFriends, userId)   

    def reducer(self, key, values):
        items = list(values)
        yield key, (min(items), max(items))
    

if __name__ == '__main__':
    AverageByRange.run()
