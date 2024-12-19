from mrjob.job import MRJob   

class QuantityByCustomer(MRJob): 
    """ The goal of this mapreduce job is to use the data files in /dataset/orders
    We will need to differentiate between the different files because we have different tables in each file with different numbers of variables.
    The output for this map reduce will be the key customerID and the value will be a tuple containing the country and total quantity"""

    def mapper(self, key, line):
        """In the mapper we will have to differentiate between the different data files. 
        We have three different types of files: customers (2 variables), items (3 variables), and orders (5 variable).
        To decide which file we have we will differentiate by the length of each line.
        Once we do this we will only collect the useful observations and yield the customerID, country, and quantity"""
        row = line.split("\t")
        if len(row) == 2: #indicates the customer table
            if row[0] != "CustomerID": #check for the table header
                (customerid, country) = (row[0],row[1])
                yield customerid, country
        elif len(row) == 5: #indicates the orders table
            if row[0] != "InvoiceNo": #check for the table header
                (quantity, customerid) = (row[2],row[4])
                quantity = int(quantity)
                yield customerid, quantity
        else: 
            self.set_status("alive") #sends heartbeat message
    

    def reducer(self, customerid, values):
        """In the reducer, we will need to combine all of the quantities that belong to each customer and yield them"""
        vals = list(values)
        total_quantity = 0
        for v in vals:
            if isinstance(v,int):
                total_quantity += v #if the item is integer, add to quantity
        for v in vals:
            if isinstance(v,str):
                country = v
                break # we can break the loop because we know the country will be the same for all instances of the customerid
        yield customerid, (country, total_quantity)

if __name__ == '__main__':
    QuantityByCustomer.run()  

### Bugs Detected and Fixed ###
# 1. Changed the name of the class to be more descriptive, and changed name in main
# 2. edited class docstring added them to mapper and reducer
# 3. adding if statement to the mapper to differentiate files and proper linesplitting
# 4. assign the values iterator to a list in reducer
# 5. change the reducer to contain a loop that will correctly assign the total quantity.
# I am not sure why we have the country in the value because none of the customers would be repeated, meaning that each customerid only has one corresponding country. I think that a compound key with customerid and country should have been used. I didn't change it because the autograder worked correctly this way.
