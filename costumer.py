from mrjob.job import MRJob

class MrCustomer(MRJob):
    def mapper(self, _, line):
        userId, _, price = line.split(",")           
        
        yield userId, float(price) 

    def reducer(self, userId, values):
        yield userId, sum(values)



if __name__ == '__main__':
    MrCustomer.run()