from datetime import datetime

import dlt
import pandas as pd
import scrapy


class AquaticplantsSpider(scrapy.Spider):
    name = "aquaticplants"
    data = []

    start_urls = ["https://aquaticplants.co.in/aquatic-plants/"]

    def parse(self, response):
        links = []
        product_divs = response.css(".t-entry-visual-cont")
        for product_div in product_divs:
            href = product_div.css("a::attr(href)").get()
            links.append(href)

        for link in links:
            yield response.follow(link, callback=self.parse_product)

        # Pagination
        next_page = response.css("nav.loadmore-button a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_product(self, response):
        product_name = response.css("h1.product_title::text").get()
        product_price = response.css(
            ".price ins ins span.woocommerce-Price-amount::text"
        ).get()
        if product_price is None:
            product_price = response.css(
                ".price span.woocommerce-Price-amount::text"
            ).get()
        categories_ = response.css(".detail-value a::text").getall()
        categories = ", ".join(line.strip() for line in categories_ if line.strip())
        desc = response.css(".product-tab p::text").getall()
        description = " ".join(desc).strip()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.data.append(
            {
                "Timestamp": current_time,
                "Name": product_name,
                "Price": product_price,
                "Categories": categories,
                "Description": description,
            }
        )

    def closed(self, reason):
        print("Scrapy data:")
        json = self.DataFrame(
            self
        )  # Call the DataFrame method when the spider is closed
        df = pd.DataFrame(json)  # view dataframe
        print(df)
        print(list(self.DataFrame(self)))

        self.aquaticplants_pipeline(df=json)

    @dlt.resource(
        table_name="aquaticplants_products",
        write_disposition="replace",
    )
    def DataFrame(self):
        df = pd.DataFrame(self.data)
        yield df.to_dict(orient="records")

    def aquaticplants_pipeline(self, df):
        pipeline = dlt.pipeline(
            pipeline_name="e-commerce",
            destination="duckdb",
            dataset_name="integrations",
        )
        load_info = pipeline.run(df)
        print(load_info)


if __name__ == "__main__":
    pass
