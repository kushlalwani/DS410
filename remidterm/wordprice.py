from mrjob.job import MRJob   # MRJob version

class WordPrice(MRJob):  #MRJob version
    """This MRJob code will use the orders dataset to find out how much weight each word in the decription shoudl get of the price of the item. For example, in an item that has a four word long description and a price of 2. Each word will get weight of 0.5"""
    def mapper(self, key, line):
        """The first mapper will get the input from the files in the orders dataset. The mapper will need to differentiate between the different tables by checking the number of columns in each table. The output will be each description of the item as a key and the price of the item as a value"""
        row = line.split("\t")
        if len(row) == 3: #items table
            if row[0] != "StockCode": #checks for the table header
                (desc,price) = (row[1],float(row[2]))
                words = desc.split()
                weighted = price/len(words)
                for word in words:
                    yield (word, [weighted,1])
        else:
            self.set_status("alive") #sends heartbeat message


    def combiner(self, key, values):
        vals = list(values) # save iterator as a list
        price = 0
        count = 0
        for val in vals:
            price = price + val[0]
            count = count + val[1]
        yield (key, [price, count])

    def reducer(self, key, values):
        """In the reducer we will find the total cost of each item and the amount of times that it appears. The input will be the word as a key and the values will be a list of weighted prices. The output of the reducer will be key value pairs with the word as the key and the values will be a list containing the total price and the amount of times a word appears if it is less than 200"""
        vals = list(values) # save the iterator as a list
        rounded_price = 0
        count = 0
        for val in vals:
            rounded_price =  rounded_price + val[0] # round in the price in the reducer
            count = count + val[1]
        if count < 200:
            yield (key, (round(rounded_price,4), count))

if __name__ == '__main__':
    WordPrice.run()   # MRJob version
