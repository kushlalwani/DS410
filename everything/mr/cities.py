from mrjob.job import MRJob # MRJob version

class doCities(MRJob):
    def mapper(self, key, line):
        
        words = line.split("\t")

        if len(words) == 7:
            if words[0] != "City" and words[5] != "Zip Codes (space seperated)":
                zipcodes = words[5]
                if zipcodes != "":
                    ziplist = zipcodes.split(" ")
                    zipcount = len(ziplist)
                else:
                    zipcount = 0
                yield (zipcount, 1)

    def combiner(self, key, values):
        yield (key, sum(values))

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    doCities.run()

        
