from mrjob.job import MRJob   # MRJob version

class MonthlyCountrySales(MRJob):  #MRJob version
    """The purpose of this map reduce job is to identify the total amount of money spent on toys and also the maximum price of one toy that was purchased by every country in each month"""
    def mapper(self, key, line):
        toy = line.split('\t')
        if toy[3] != "Quantity" and toy[4] != "InvoiceDate" and toy[5] != "UnitPrice" and toy[7] != "Country": #checks for and ignores the header of the table
            date = toy[4].split('/')
            yield ([toy[7],date[0]],[int(toy[3])*float(toy[5]),float(toy[5])])

    def reducer(self, key, values):
        vals = list(values)
        total = 0
        max_price = 0
        for item in vals: #loop used to count total price of all items in list
            total = total + item[0]
            max_price = max(max_price,item[1]) #compares the max unit price in the value list to the current maximum and updates based on the new max
        yield (key, [total,max_price])

    def combiner(self, key, values): # essentially the same code as the reducer
        total = 0
        max_price = 0
        for item in values:
            total = total + item[0]
            max_price = max(max_price,item[1])
        yield key, [total, max_price]

if __name__ == '__main__':
    MonthlyCountrySales.run()   # MRJob version
