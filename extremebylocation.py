from mrjob.job import MRJob

class ExtremeByLocation(MRJob):
    def mapper(self, _, line):
        _local, _id, _type, _temp, _, _, _, _ = line.split(',')

        if(_type.upper() in ('TMIN', 'TMAX')):
            yield (_local, _type), (float(_temp))

    def reducer(self, key, values):
        items = list(values)

        location = key[0]
        metric = key[1]

        if metric == 'TMAX':
            yield location, min(items)
        else:
            yield location, max(items)

if __name__ == '__main__':
    ExtremeByLocation.run()
