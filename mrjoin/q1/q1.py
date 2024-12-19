from mrjob.job import MRJob
from mrjob.job import MRStep

class QuantityByCountryMonth(MRJob):

    def mapper_custid(self, key, line):
        row = line.split("\t")
        if len(row) == 2: #indicates the customer table
            if row[0] != "CustomerID": #check for the table header
                (customerid, country) = (row[0],row[1])
                yield customerid, ("customer",country)
        elif len(row) == 5: #indicates the orders table
            if row[0] != "InvoiceNo": #check for the table header
                (quantity, date, customerid) = (row[2],row[3].split("/"),row[4])
                quantity = int(quantity)
                yield customerid, ("order",quantity,date[0])
        else: 
            self.set_status("alive") #sends heartbeat message
    

    def reducer_custid(self, customerid, values):
        vals = list(values)
        country = None
        orders = []
        for v in vals:
            if v[0] == "customer":
                country = v[1]
            elif v[0] == "order":
                orders.append(v[1:])

        if country: #only executes if there is a value in country
            for o in orders:
                quantity, month = o
                yield (country, month), quantity

    def reducer_country(self, country_month, values):
        total_quantity = sum(values)
        yield country_month, total_quantity

    def steps(self):
        return [MRStep(mapper=self.mapper_custid, reducer=self.reducer_custid),MRStep(reducer=self.reducer_country)]

if __name__ == '__main__':
    QuantityByCountryMonth.run()  

