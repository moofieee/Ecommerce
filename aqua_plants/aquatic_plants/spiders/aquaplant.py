from datetime import datetime
import dlt
import pandas as pd
import scrapy


class Aquaplant(scrapy.Spider):
    name = "aquaplant"

    def start_requests(self):
        urls = ["https://shop.aquaplantstudio.com/products/"]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    data = []

    def parse(self, response):
        links = []
        product_divs = response.css(".product-wrapper")
        for product_div in product_divs:
            href = product_div.css("h3 a::attr(href)").get()
            links.append(href)
        for link in links:
            yield response.follow(link, callback=self.get_data)

        for page_number in range(17):
            next_page_url = (
                f"https://shop.aquaplantstudio.com/products/page/{page_number}/"
            )
            yield response.follow(next_page_url, callback=self.parse)

    def get_data(self, response):
        product_name_ = response.css("div.summary-inner h1.product_title::text").get()
        product_name = product_name_.strip()
        product_price = response.css(".price span bdi::text").get()
        product_sku_ = response.css("span.sku::text").get()
        product_sku = product_sku_.strip()
        product_categories_ = response.css("span.posted_in a::text").getall()
        product_categories = ", ".join(
            line.strip() for line in product_categories_ if line.strip()
        )
        product_tag = response.css("span.tagged_as a::text").get()
        product_description = response.css(".wd-accordion-item div div p::text").get()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.data.append(
            {
                "Timestamp": current_time,
                "Name": product_name,
                "Price": product_price,
                "SKU": product_sku,
                "Categories": product_categories,
                "tag": product_tag,
                "Description": product_description,
            }
        )

    def closed(self, reason):
        json = self.DataFrame(
            self
        )  # Call the DataFrame method when the spider is closed
        df = pd.DataFrame(json)  # view dataframe
        print(df)
        print(list(self.DataFrame(self)))

        self.aquaplant_pipeline(df=json)

    @dlt.resource(
        table_name="aquaplant_products",
        write_disposition="replace",
    )
    def DataFrame(self):
        df = pd.DataFrame(self.data)
        yield df.to_dict(orient="records")

    def aquaplant_pipeline(self, df):
        pipeline = dlt.pipeline(
            pipeline_name="e-commerce",
            destination="duckdb",
            dataset_name="integrations",
        )
        load_info = pipeline.run(df)
        print(load_info)


if __name__ == "__main__":
    pass
