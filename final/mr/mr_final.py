from mrjob.job import MRJob

class doFinal(MRJob):
    """This class will do map reduce on the facebook dataset, after analyzing the dataset, I found that there are no nulls in either of the columns, there are also no self-edges in the dataset, so we do not need to worry about dealing with self edges"""

    def mapper(self, key, line):
        """This will map the lines in the dataset into key value pairs, the key will be the id and the values will be a list with a one in the left count and the right count"""
        splitlines = line.split(" ")
        if len(splitlines) == 2:
            ID1, ID2 = splitlines
            yield (ID1,[1,0]) #Will yield 1 for when the ID appears on the left side
            yield (ID2, [0,1]) #yields a 1 for when the ID appears on the rights side

    def combiner(self, key, values):
        total_left = 0
        total_right = 0

        vals = values
        for val in vals: # count up all the ones in both the left and right sides
            total_left += val[0]
            total_right += val[1]

        yield (key, [total_left, total_right])

    def reducer(self, key, values):
        total_left = 0
        total_right = 0

        vals = values
        for val in vals: # sum up the counts on both the left and right sides
            total_left += val[0]
            total_right += val[1]
        if total_left + total_right > 2:
            yield (key, [total_left, total_right])


if __name__ == '__main__':
    doFinal.run()
