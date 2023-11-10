from datetime import datetime

import dlt
import pandas as pd
import scrapy


class AquabynatureSpider(scrapy.Spider):
    name = "finsnflora"
    data = []

    start_urls = ["https://finsnflora.com/29-aquarium-live-stock"]

    def parse(self, response):
        links = []
        product_divs = response.css(".thumbnail-container")
        for product_div in product_divs:
            href = product_div.css("a::attr(href)").get()
            links.append(href)

        for link in links:
            yield response.follow(link, callback=self.parse_product)

        #         # Pagination
        next_page = response.css("ul.page-list li.current + li a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_product(self, response):
        product_title = response.css("h1.page-title span::text").get()
        product_price_ = response.css(
            "span.product-price.current-price-value::text"
        ).get()
        product_price = product_price_.strip()
        product_description_ = response.css(
            ".product-description .rte-content p::text"
        ).getall()
        product_description = ", ".join(
            line.strip() for line in product_description_ if line.strip()
        )
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.data.append(
            {
                "Timestamp": current_time,
                "Name": product_title,
                "Price": product_price,
                "description": product_description,
            }
        )

    def closed(self, reason):
        print("Scrapy data:")
        json = self.DataFrame(self)
        df = pd.DataFrame(json)  # view dataframe
        print(df)
        # print(list(self.DataFrame(self)))

        self.finsnflora_pipeline(df=json)

    @dlt.resource(
        table_name="finsnflora_products",
        write_disposition="replace",
    )
    def DataFrame(self):
        df = pd.DataFrame(self.data)
        yield df.to_dict(orient="records")

    def finsnflora_pipeline(self, df):
        pipeline = dlt.pipeline(
            pipeline_name="e-commerce",
            destination="duckdb",
            dataset_name="integrations",
        )
        load_info = pipeline.run(df)
        print(load_info)


if __name__ == "__main__":
    pass
