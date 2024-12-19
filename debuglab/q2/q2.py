from mrjob.job import MRJob

class StateCityInfo(MRJob):
    """This map reduce code will determine information about each state in the USA
    from the output we should recieve the followign information about each state with the state name being the key
    1: total number of cities in the state
    2: total population of the state
    3: the max number of zipcodes in one city in each state"""
    def mapper(self, key, line):
        """ 
        This mapper expects the following information as input:
        key: The key is a unique identifier for each line in the file we are reading, not important in this case
        value: The value is each line of our file, contains information about each city
        This will yield the key which will be the name of the state and the value will be a list of the population of the city and the number of zipcodes in the city
        """
        stateInfo = line.split("\t")
        if len(stateInfo) == 7 and stateInfo[4].isnumeric(): # check for the header and to ensure we have a complete row of data
            (state,population,zipList) = (stateInfo[2],stateInfo[4],stateInfo[5].split(" ")) # the zipcodes are seperated by spaces so we split by space to get a list of zipcodes
            yield state, (int(population), len(zipList)) # casting the population to int and find the number of zipcodes using len()
        else:
            self.set_status("alive")
    def reducer(self, state, values):
        """
        The reducer takes in the key (state) and the list of values (population and number zipcodes)
        We will calculate the total number of population by each state and find the city that the most zip codes
        The yielded information will be the key, state, with a list of values containing the number of cities, total population, and the highest number of zipcodes in one city whithin the state
        """
        vals = list(values) # assign to a list because its a iterator
        num_cities = len(vals) # the total length of the values list is the number of cities we have in the state
        population = 0
        maxZip = 0
        for (pop,zipCount) in vals: # iterate through the list of tuples to calculate what we need
            population += pop 
            maxZip = max(maxZip, zipCount)
        yield state, (num_cities, population, maxZip)

if __name__ == '__main__':
    StateCityInfo.run()
### Bugs detected and Fixed ###
# 1. removed a bunch of random spaces/tabs at end of lines
# 2. changed name of class in the header and the main function
# 3. added docstring to class and edited the ones under mapper and reducer
# 4. Since the cities file is a 'c s v' so we will need to split by tabs instead of spaces
# 5. need to add an if statement to check for the header in the mapper
# 6. changed the names of unclear variables in both the mapper and the reducer
# 7. got rid of the city variable in the mapper because it is not needed
# 8. store the iterator values in a list in the reducer because we need multiple iterations
# 9. initialize the population and the maxZip variables as counter
