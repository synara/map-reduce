from mrjob.job import MRJob
from mrjob.step import MRStep


class AverageByAge(MRJob):
    def mapper(self, _, line):
        userId, name, age, numberOfFriends = line.split(",") 
        yield age, int(numberOfFriends)         


    def reducer(self, key, values):
        items = list(values)

        #gera todo mundo pra mesma chave e inverte a media e a idade para facilitar a ordenação
        yield None, ((sum(items)/len(items)), key) 

    def avgbyages(self, key, values):
        items = sorted(list(values)) #ordena pela media
        first = items[0] #pega o primeiro item

        #exibe primeiro a idade, depois a media
        yield first[1], first[0]
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(
                reducer=self.avgbyages)
        ]


if __name__ == '__main__':
    AverageByAge.run()
