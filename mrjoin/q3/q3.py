from mrjob.job import MRJob
from mrjob.job import MRStep

class OrderParts(MRJob):

    def mapper_invoice(self, key, line):
        row = line.split("\t")
        if len(row) == 5: #indicates the customer table
            if row[0] != "InvoiceNo": #check for the table header
                invoice = row[0]
                yield invoice, 1
        else: 
            self.set_status("alive") #sends heartbeat message
    
    def reducer_invoice(self, invoice, values):
        parts = sum(values)
        yield(parts,1)

    def reducer_parts(self, parts, count):
        orders = sum(count)
        yield parts, orders

    def steps(self):
        return [MRStep(mapper=self.mapper_invoice, reducer=self.reducer_invoice),MRStep(reducer=self.reducer_parts)]

if __name__ == '__main__':
    OrderParts.run()  

