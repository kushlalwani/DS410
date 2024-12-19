from mrjob.job import MRJob   # MRJob version

class WordCountLongerThan4(MRJob):  #MRJob version
    """In this map reduce we want to count the number of words in a file that are longer than four characters"""
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            if len(w) > 4: #check if the length of the word is greater than 4
                yield (w, 1)

    def combiner(self, word, values):
        yield word, sum(values) #yields the word and the count of appearances

    def reducer(self, word, values): #same code as the combiner
        yield (word, sum(values))

if __name__ == '__main__':
    WordCountLongerThan4.run()   # MRJob version
