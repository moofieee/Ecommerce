from datetime import datetime

import dlt
import pandas as pd
import scrapy


class Sreepadma(scrapy.Spider):
    name = "sreepadma"
    data = []

    def start_requests(self):
        urls = ["https://www.sreepadma.com/all-plant"]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        links = []
        product_divs = response.css(".product-card")
        for product_div in product_divs:
            href = product_div.css("a::attr(href)").get()
            links.append(href)

        for link in links:
            yield response.follow(link, callback=self.get_data)

    def get_data(self, response):
        product_name = response.css(".details-content h3 a::text").get()
        product_price = response.css(".details-list-group table tr td::text").extract()[
            2
        ]
        product_sku = response.css(".details-list-group table tr td::text").extract()[1]
        product_description = response.css(".product-details-frame div p::text").get()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.data.append(
            {
                "Timestamp": current_time,
                "Name": product_name,
                "Price": product_price,
                "SKU": product_sku,
                "Description": product_description,
            }
        )

    def closed(self, reason):
        json = self.DataFrame(self)
        df = pd.DataFrame(json)  # view dataframe
        print(df)
        print(list(self.DataFrame(self)))

        self.sreepadma_pipeline(df=json)

    @dlt.resource(
        table_name="sreepadma_products",
        write_disposition="replace",
    )
    def DataFrame(self):
        df = pd.DataFrame(self.data)
        yield df.to_dict(orient="records")

    def sreepadma_pipeline(self, df):
        pipeline = dlt.pipeline(
            pipeline_name="e-commerce",
            destination="duckdb",
            dataset_name="integrations",
        )
        load_info = pipeline.run(df)
        print(load_info)


if __name__ == "__main__":
    pass
