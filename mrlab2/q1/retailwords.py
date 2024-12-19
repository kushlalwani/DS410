from mrjob.job import MRJob   # MRJob version

class ToyDescriptionWordCount(MRJob):  #MRJob version
    def mapper(self, key, line):
        toyCols = line.split('\t')
        if toyCols[2] != "Description":
            desc = toyCols[2].split()
            for word in desc:
                yield (word, 1)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    ToyDescriptionWordCount.run()   # MRJob version
