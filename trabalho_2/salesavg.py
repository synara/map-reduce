from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter
from datetime import datetime as dt

class AvgSales(MRJob):
    window = 3

    def mapper(self, _, line):
        sale = line.split(',')

        transaction_date = sale[0]
        product = sale[1]
        price = sale[2]
        country = sale[7]

        timestamp = dt.strptime(transaction_date, '%m/%d/%y %H:%M').strftime("%m/%d/%Y")

        #if(country.lower() == "south africa"):
        yield (country, product, timestamp), (float(price))

        
    def reducer(self, key, values):
        items = list(values)

        #soma os valores para o caso de ter dias e produtos iguais
        #e passa o timestamp como "chave" da segunda tupla pra ordenar pela data
        yield (key[0], key[1]), (key[2], sum(items))

    def reducer_2(self, key, values):

        items = sorted(list(values))

        sum = 0.0
        for i in range(len(items)):
            item = items[i]
            timestamp = item[0]
            price = item[1]

            sum += price
                                
            if(i + 1) > self.window:
                sum -= items[i-self.window][1]
            
            q = min(i + 1, self.window)
            
            moving = sum / q

            yield key, (item, moving)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer_2)
        ]

if __name__ == "__main__":
    AvgSales.run()