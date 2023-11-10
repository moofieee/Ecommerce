from datetime import datetime

import dlt
import pandas as pd
import scrapy


class Geturpet(scrapy.Spider):
    name = "geturpet"
    data = []

    def start_requests(self):
        urls = ["https://geturpet.com/aquatic-plants/"]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        links = []
        product_divs = response.css(".product-inner")
        for product_div in product_divs:
            href = product_div.css("div a::attr(href)").get()
            links.append(href)

        for link in links:
            yield response.follow(link, callback=self.get_data)

    def get_data(self, response):
        product_name_ = response.css(".product-summary-wrap div div h2::text").get()
        product_name = product_name_.strip()
        product_price = response.css(".price ins span bdi::text").get()
        if product_price is None:
            product_price = response.css(".price del span bdi::text").get()
        categories_ = response.css(".product_meta span.posted_in a::text").getall()
        categories = ", ".join(line.strip() for line in categories_ if line.strip())
        product_sku = response.css(".product_meta span span.sku::text").get()
        product_description = response.css(
            ".product-summary-wrap .description.woocommerce-product-details__short-description p::text"
        ).get()
        product_availability = response.css(".product_meta span span.stock::text").get()
        if product_availability is None:
            product_availability = "Available"
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.data.append(
            {
                "Timestamp": current_time,
                "Name": product_name,
                "Price": product_price,
                "product_sku": product_sku,
                "availability": product_availability,
                "Categories": categories,
                "Description": product_description,
            }
        )

    def closed(self, reason):
        print("Scrapy data:")
        json = self.DataFrame(self)
        df = pd.DataFrame(json)  # view dataframe
        print(df)
        print(list(self.DataFrame(self)))

        self.geturpet_pipeline(df=json)

    @dlt.resource(
        table_name="geturpet_products",
        write_disposition="replace",
    )
    def DataFrame(self):
        df = pd.DataFrame(self.data)
        yield df.to_dict(orient="records")

    def geturpet_pipeline(self, df):
        pipeline = dlt.pipeline(
            pipeline_name="e-commerce",
            destination="duckdb",
            dataset_name="integrations",
        )
        load_info = pipeline.run(df)
        print(load_info)


if __name__ == "__main__":
    pass
