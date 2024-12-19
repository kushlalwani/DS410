from mrjob.job import MRJob   # MRJob version

class WordCount(MRJob):  #MRJob version
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            if len(w) > 4:
                yield (w, 1)

    def reducer(self, key, values):
        count = sum(values)
        if count > len(key):
            yield (key, count)

if __name__ == '__main__':
    WordCount.run()   # MRJob version
