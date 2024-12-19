from mrjob.job import MRJob   # MRJob version

class WordCount(MRJob):  #MRJob version
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            for position,character in enumerate(w):
                yield ((character,position), 1)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    WordCount.run()   # MRJob version
