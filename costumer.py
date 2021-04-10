from mrjob.job import MRJob

##para canalizar para um arquivo de saída 
##'>' sobrescreve e '>>' ele acrescenta
##python .\costumer.py .\customer-orders.csv > amoutbycostumer.txt 

class AmountByCostumer(MRJob):
    def mapper(self, _, line):
        userId, _, price = line.split(",")           
        
        yield userId, float(price) 

    def reducer(self, userId, values):
        #yield userId, sum(values)
        #yield userId, min(values)
        #yield userId, prices
        prices = list(values)
        yield userId, (max(prices), min(prices), sum(prices)/len(prices))
        


if __name__ == '__main__':
    AmountByCostumer.run()