from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter


class Sales(MRJob):
    def mapper(self, _, line):
        sale = line.split(',')
        transaction_date = sale[0]
        product = sale[1]
        price = sale[2]
        payment_type = sale[3]
        name = sale[4]
        city = sale[5]
        state = sale[6]
        country = sale[7]
        account_created = sale[8]
        last_login = sale[9]
        latitude = sale[10]
        longitude = sale[11]

        yield country, product

    def reducer(self, key, values):
        items = list(values)
        yield None, (key, items)

    def reducer_2(self, key, values):
        items = list(values)
        
        for i in range(len(items)):
            item = items[i]
            country = item[0]
            products = item[1]

            countproducts = Counter(products)

            yield country, countproducts
       
        
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer_2)
        ]


if __name__ == '__main__':
    Sales.run()