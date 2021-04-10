from mrjob.job import MRJob 

class MovingAverage(MRJob):
    window = 3

    def mapper(self, _, line):
        company, timestamp, value = line.split(',')

        yield company, (timestamp, float(value))

    def reducer(self, key, values):
        items = sorted(list(values))

        sum = 0.0
        for i in range(len(items)):
            item = items[i]
            timestamp = item[0]
            value = item[1]

            sum +=  value

            if(i + 1) > self.window:
                sum -= items[i-self.window][1]
            
            q = min(i + 1, self.window)
            
            moving = sum / q

            yield key, (item, moving)

if __name__ == '__main__':
    MovingAverage.run()