from mrjob.job import MRJob   # MRJob version

class WordCountAndLength(MRJob):  #MRJob version
    """This map reduce job aims to see how many words in the file have length that are greater than 4. Then is checks if the length of the word is greater than the total count of the words. If the count is greater than the length of the word, only then the word and count will be shown in the final output"""
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            if len(w) > 4: # checks is the length of the word is greater than 4
                yield (w, 1)
    
    def reducer(self, word, values):
        count = sum(values)
        if count > len(word): #check if the appearances of the word is greater than the length of the word
            yield word, count

    def combiner(self, word, values): #only sums up the count of words
        yield word, sum(values)

if __name__ == '__main__':
    WordCountAndLength.run()   # MRJob version
