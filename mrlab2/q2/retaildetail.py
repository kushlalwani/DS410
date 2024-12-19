from mrjob.job import MRJob   # MRJob version

class MonthlyCountrySales(MRJob):  #MRJob version
    def mapper(self, key, line):
        toy = line.split('\t')
        if toy[3] != "Quantity" and toy[4] != "InvoiceDate" and toy[5] != "UnitPrice" and toy[7] != "Country":
            date = toy[4].split('/')
            yield ([toy[7],date[0]],[int(toy[3])*float(toy[5]),float(toy[5])])

    def reducer(self, key, values):
        vals = list(values)
        total = 0
        max_price = 0
        for item in vals:
            total = total + item[0]
            max_price = max(max_price,item[1])
        yield (key, [total,max_price])

if __name__ == '__main__':
    MonthlyCountrySales.run()   # MRJob version
