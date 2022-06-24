# voor ean etc dit vanwege scrapetijd > 3 uur , en omdat het nauwelijks veranderd

from datetime import datetime
import pandas as pd
import os
from requests_html import HTMLSession
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import configparser
from pathlib import Path

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")

session = HTMLSession()

agk = pd.read_csv(max(Path.cwd().glob("AGK_gescrapte_*.csv"), key=os.path.getctime))

dfcols = ["product_nr", "ean", "merknaam", "basis_info", "afbeelding"]
product_info = pd.DataFrame(columns=dfcols)

for index, row in agk.iterrows():
    item_url = row["url"]
    product_nr = row["product_nr"]
    r = session.get(item_url)
    ean = (r.html.xpath("//th[contains(text(),'Ean code')]/../td/text()") or [""])[0]
    merknaam = (r.html.xpath("//th[contains(text(),'merk')]/../td/text()") or [""])[0]
    basis_info = ((r.html.xpath("//div[@class='meerinfo']/text()") or [""])[0]).encode("ascii", "ignore").decode("ascii")
    afbeelding = (r.html.xpath("//div[@class='product-image']/a/@href") or [""])[0]
    product_info = product_info.append(
        pd.Series([product_nr, ean, merknaam, basis_info, afbeelding,], index=dfcols,), ignore_index=True,
    )

config_db = dict(
    drivername="mariadb",
    username=alg_config.get("database leveranciers", "user"),
    password=alg_config.get("database leveranciers", "password"),
    host=alg_config.get("database leveranciers", "host"),
    port=alg_config.get("database leveranciers", "port"),
    database=alg_config.get("database leveranciers", "database"),
)
engine = create_engine(URL.create(**config_db))

product_info.to_sql("AGK_detailed_product_info_", con=engine, if_exists="replace", index=False, chunksize=1000)

with engine.connect() as con:

    aantal_items = con.execute("SELECT count(*) FROM AGK_detailed_product_info_").fetchall()[-1][-1]
    leverancier = "detailed_product_info_AGK"
    sql_insert = "INSERT INTO process_import_log_extra_info (aantal_items, leverancier) VALUES (%s,%s)"
    con.execute(sql_insert, (aantal_items, leverancier))

engine.dispose()

date_now = datetime.now().strftime("%c").replace(":", "-")
product_info.to_csv("AGK_basisinfo_" + date_now + "_month" + ".csv", index=False)
