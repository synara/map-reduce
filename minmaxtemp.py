from mrjob.job import MRJob

class Temperature(MRJob):
    def mapper(self, _, line):
        _local, _id, _type, _temp, _, _, _, _ = line.split(',')

        if(_type.upper() in ('TMIN', 'TMAX')):
            yield _local, (float(_temp))

    def reducer(self, key, values):
        items = list(values)

        yield key, (min(items), max(items))

if __name__ == '__main__':
    Temperature.run()

