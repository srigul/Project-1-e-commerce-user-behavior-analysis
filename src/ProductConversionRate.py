from mrjob.job import MRJob
from mrjob.step import MRStep

class ProductConversionRate(MRJob):

    def mapper(self, _, line):
        # Split each line by comma
        fields = line.strip().split(',')
        
        # Handle user_activity.csv with 5 fields
        if len(fields) == 5:
            activity_type = fields[2]   # Interaction type like browse, add_to_cart, or purchase
            product_id = fields[3]
            
            if activity_type in ["browse", "add_to_cart"]:
                yield product_id, ('interaction', None)
            elif activity_type == "purchase":
                yield product_id, ('purchase', None)

        # Handle transactions.csv with 7 fields
        elif len(fields) == 7:
            product_id = fields[3]
            product_category = fields[2]  # Product category
            yield product_id, ('purchase', product_category)

    def reducer_product_conversion(self, product_id, values):
        interactions = 0
        purchases = 0
        category = None

        # Count interactions and purchases for each product ID
        for value_type, category_value in values:
            if value_type == 'interaction':
                interactions += 1
            elif value_type == 'purchase':
                purchases += 1
                if category_value:
                    category = category_value

        if category and interactions > 0:
            conversion_rate = purchases / interactions
            yield category, round(conversion_rate, 2)

    def reducer_category_average(self, category, conversion_rates):
        total_conversion_rate = 0
        count = 0

        # Sum up the conversion rates and count the products per category
        for conversion_rate in conversion_rates:
            total_conversion_rate += conversion_rate
            count += 1

        # Calculate average conversion rate per category
        if count > 0:
            average_conversion_rate = total_conversion_rate / count
            yield category, round(average_conversion_rate, 2)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_product_conversion),
            MRStep(reducer=self.reducer_category_average)
        ]

if __name__ == '__main__':
    ProductConversionRate.run()
