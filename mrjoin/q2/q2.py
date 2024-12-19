from mrjob.job import MRJob
from mrjob.job import MRStep

class CountryByCode(MRJob):

    def mapper_custid(self, key, line):
        row = line.split("\t")
        if len(row) == 2: #indicates the customer table
            if row[0] != "CustomerID": #check for the table header
                (customerid, country) = (row[0],row[1])
                yield customerid, ("customer",country)
        elif len(row) == 5: #indicates the orders table
            if row[0] != "InvoiceNo": #check for the table header
                (code,customerid) = (row[1],row[4])
                yield customerid, ("order",code)
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
                orders.append(v[1])

        if country: #only executes if there is a value in country
            for code in orders:
                yield code, country

    def reducer_code(self, stockcode, countries):
        distinct = set(countries)
        yield stockcode, len(distinct)

    def steps(self):
        return [MRStep(mapper=self.mapper_custid, reducer=self.reducer_custid),MRStep(reducer=self.reducer_code)]

if __name__ == '__main__':
    CountryByCode.run()  

