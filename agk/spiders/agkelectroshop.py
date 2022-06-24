# scrapy crawl agkelectroshop -o item.csv
# scrapy shell www.agkelektroshop.nl

import os
from datetime import datetime

import dropbox
import pandas as pd
import scrapy
from scrapy.http import FormRequest, Request
from scrapy_splash import SplashRequest
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import configparser
from pathlib import Path
import sys

sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")
dbx_api_key = alg_config.get("dropbox", "api_dropbox")
dbx = dropbox.Dropbox(dbx_api_key)
config_db = dict(
    drivername="mariadb",
    username=alg_config.get("database leveranciers", "user"),
    password=alg_config.get("database leveranciers", "password"),
    host=alg_config.get("database leveranciers", "host"),
    port=alg_config.get("database leveranciers", "port"),
    database=alg_config.get("database leveranciers", "database"),
)
current_folder = Path.cwd().name.upper()
export_config = configparser.ConfigParser(interpolation=None)
export_config.read(Path.home() / "bol_export_files.ini")
korting_percent = int(export_config.get("stap 1 vaste korting", current_folder.lower()).strip("%"))


class AgkelectroshopSpider(scrapy.Spider):
    name = "agkelectroshop"
    allowed_domains = ["agkelektroshop.nl"]
    start_urls = ["https://www.agkelektroshop.nl"]

    def parse(self, response):
        inlog_url = "https://www.agkelektroshop.nl/customer/account/login/"
        script = """
        function main(splash, args)
            splash:init_cookies(splash.args.cookies)
            assert(splash:go(args.url))
            assert(splash:wait(4))
            email = splash:select('#email')
            email:send_text(splash.args.email)
            assert(splash:wait(2.5))
            password = splash:select('#pass')
            password:send_text(splash.args.password)
            assert(splash:wait(5))
            assert(splash:runjs('document.querySelector("#send2").click()'))
            assert(splash:wait(5))
            return {
                html = splash:html(),
                png = splash:png(),
                cookies = splash:get_cookies(),
            }
        end
        """
        yield SplashRequest(
            inlog_url,
            session_id="foo",
            endpoint="execute",
            args={
                "lua_source": script,
                "email": alg_config.get("agk website", "email"),
                "password": alg_config.get("agk website", "password"),
            },
            callback=self.scrape_pages,
        )

    def scrape_pages(self, response):
        script = """
                function main(splash)
                    splash:init_cookies(splash.args.cookies)
                    assert(splash:go(splash.args.url))
                    assert(splash:wait(5))
                    return {
                        cookies = splash:get_cookies(),
                        html = splash:html()
                    }
                  end
            """
        categorys = response.xpath("//div[@class='main-categories']/ul/li/ul/li/a/@href").getall()
        for category in categorys:
            yield SplashRequest(
                url=category,
                session_id="foo",
                callback=self.artikelen,
                method="GET",
                endpoint="execute",
                args={"lua_source": script},
            )

    def artikelen(self, response):
        script = """
                function main(splash)
                    splash:init_cookies(splash.args.cookies)
                    assert(splash:go(splash.args.url))
                    assert(splash:wait(5))
                    return {
                        cookies = splash:get_cookies(),
                        html = splash:html()
                    }
                  end
            """
        artikelen = response.xpath("//div[@class='products-block hover-block']//li")
        artikelen_category = response.xpath("//div[@class='page-title category-title']/h1/text()").get()
        for artikel in artikelen:
            yield {
                "artikelen_category": artikelen_category,
                "url": self.clear_content(artikel.xpath(".//a/@href").get()),
                "product_title": self.clear_content(artikel.xpath(".//h2/a/@title").get()),
                "product_nr": self.clear_content(artikel.xpath(".//p/text()").get()),
                "voorraad": self.clear_content(artikel.xpath(".//p/@title").get()),
                "prijs": self.clear_content_numbers(artikel.xpath(".//p[@class='special-price']/span/text()").get()),
            }
        next_page = response.xpath("//a[@class='next i-next']/@href").get()
        if next_page:
            yield SplashRequest(
                url=next_page,
                session_id="foo",
                callback=self.artikelen,
                method="GET",
                endpoint="execute",
                args={"lua_source": script},
            )

    def clear_content(self, content):
        if content:
            return (
                content.replace("Aantal in voorraad : ", "")
                .replace("Product nr: ", "")
                .replace("\n", "")
                .replace("\r", "")
                .replace("\t", "")
                .replace("\xa0", " ")
                .strip()
                .encode("ascii", "ignore")
                .decode("ascii")
            )

    def clear_content_numbers(self, content):
        content_clean = self.clear_content(content)
        if content_clean:
            return content_clean.replace(",", ".").replace("€ ", "")

    def close(self, reason):
        date_now = datetime.now().strftime("%c").replace(":", "-")
        os.rename((max(Path.cwd().glob("*item*.csv"), key=os.path.getctime)), "AGK_gescrapte_" + date_now + ".csv")
        net_gescrapt = pd.read_csv(max(Path.cwd().glob("AGK_gescrapte_*.csv"), key=os.path.getctime)).assign(
            artikelen_category=lambda x: x["artikelen_category"].str.replace("TL buizen", "TL-lampen")
        )  # voor cat gelijk te trekken en extra te rekenen
        engine = create_engine(URL.create(**config_db))
        basis_info = pd.read_sql("AGK_detailed_product_info_", con=engine)
        agk_merged = (
            pd.merge(net_gescrapt, basis_info, on="product_nr", how="left")
            .drop_duplicates("product_nr")
            .dropna(subset=["voorraad"])
        )
        agk_merged = (
            agk_merged.assign(
                ean=lambda x: pd.to_numeric(x["ean"], errors="coerce"),
                prijs=lambda x: pd.to_numeric(x["prijs"], errors="coerce"),
                lk=lambda x: (korting_percent * x["prijs"] / 100).round(2),
            )
            .assign(prijs = lambda x: (x["prijs"] - x["lk"]).round(2))
            .query("ean == ean" or "prijs==prijs")
        )
        # agk_merged = agk_merged.loc[agk_merged["voorraad"] >= 0] # waneer ik wil filteren
        agk_merged.to_csv("AGK_samen_" + date_now + ".csv", index=False, encoding="utf-8-sig")
        agk_info = agk_merged.assign(
            eigen_sku=lambda x: "AGK" + x["product_nr"],
            advies_prijs="",
            gewicht="",
            url_plaatje="",
            url_artikel="",
            lange_omschrijving="",
            verpakings_eenheid="",
        ).rename(
            columns={
                "product_nr": "sku",
                "merknaam": "merk",
                "artikelen_category": "category",
                "afbeelding": "plaatje",
            }
        )

        akg_totaal = max(Path.cwd().glob("AGK_samen_*.csv"), key=os.path.getctime)
        with open(akg_totaal, "rb") as f:
            dbx.files_upload(
                f.read(),
                "/macro/datafiles/AGK/" + akg_totaal.name,
                mode=dropbox.files.WriteMode("overwrite", None),
                mute=True,
            )
        agk_info_db = agk_info[
            [
                "eigen_sku",
                "sku",
                "ean",
                "voorraad",
                "merk",
                "prijs",
                "advies_prijs",
                "category",
                "gewicht",
                "url_plaatje",
                "url_artikel",
                "product_title",
                "lange_omschrijving",
                "verpakings_eenheid",
                "lk",
            ]
        ]

        huidige_datum = datetime.now().strftime("%d_%b_%Y")
        agk_info_db.to_sql(
            f"{current_folder}_dag_{huidige_datum}", con=engine, if_exists="replace", index=False, chunksize=1000
        )

        with engine.connect() as con:
            con.execute(f"ALTER TABLE {current_folder}_dag_{huidige_datum} ADD PRIMARY KEY (eigen_sku(20))")
            aantal_items = con.execute(f"SELECT count(*) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
            totaal_stock = int(
                con.execute(f"SELECT sum(voorraad) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
            )
            totaal_prijs = int(
                con.execute(f"SELECT sum(prijs) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
            )
            leverancier = f"{current_folder}"
            sql_insert = "INSERT INTO process_import_log (aantal_items, totaal_stock, totaal_prijs, leverancier) VALUES (%s,%s,%s,%s)"
            con.execute(sql_insert, (aantal_items, totaal_stock, totaal_prijs, leverancier))

        engine.dispose()
