import requests
from bs4 import BeautifulSoup
import concurrent.futures
from datetime import datetime


class TikrScraper:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
    }

    def __init__(self, tikr):
        self.tikr = tikr
        self.url = f"https://finance.yahoo.com/quote/{self.tikr}/options?p={self.tikr}"

    def get_underline_price(self):
        page = requests.get(self.url, headers=self.headers)
        soup = BeautifulSoup(page.content, "html.parser")
        price = soup.find("fin-streamer", class_="Fw(b) Fz(36px) Mb(-4px) D(ib)")[
            "value"
        ]
        return float(price)

    def get_experation_urls(self, link):
        page = requests.get(link, headers=self.headers)
        soup = BeautifulSoup(page.content, "html.parser")

        new_links = (
            link + "&date=" + (tag["value"]) + "&straddle=true"
            for tag in soup.find_all("option")
        )
        return new_links

    def get_strike_url(self, link):
        page = requests.get(link, headers=self.headers)
        soup = BeautifulSoup(page.content, "html.parser")
        rows = soup.find("tbody").find_all("tr")

        strike_link = (
            "https://finance.yahoo.com"
            + row.find("td", class_="data-col5").a["href"].strip("&straddle=true")
            for row in rows
        )
        return strike_link

    def filter_generator(self, gen):
        seen = set()
        for item in gen:
            if item not in seen:
                seen.add(item)
                yield item

    def info_getter(self, link):
        page = requests.get(link, headers=self.headers)
        soup = BeautifulSoup(page.content, "html.parser")
        my_table = []
        strike = float(link.split("=")[1])
        option_types = ["calls", "puts"]

        for option_type in option_types:
            if soup.find("table", class_=option_type) is None:
                break
            else:
                table = soup.find("table", class_=option_type)
                for row in table.find_all("tr")[1:]:
                    my_row = []

                    option = option_type.strip("s")
                    expiration = datetime.strptime(
                        row.find("td", class_="data-col2").a.text,
                        "%Y-%m-%d",
                    )
                    bid = row.find("td", class_="data-col4").text
                    ask = row.find("td", class_="data-col5").text
                    volume = row.find("td", class_="data-col8").text
                    open_interest = row.find("td", class_="data-col9").text

                    my_row.append(strike)
                    my_row.append(option)
                    my_row.append(expiration)
                    if bid == "-":
                        my_row.append(float(0))
                    else:
                        my_row.append(float(bid.replace(",", "")))
                    if ask == "-":
                        my_row.append(float(0))
                    else:
                        my_row.append(float(ask.replace(",", "")))
                    if volume == "-":
                        my_row.append(int(0))
                    else:
                        my_row.append(int(volume.replace(",", "")))
                    if open_interest == "-":
                        my_row.append(int(0))
                    else:
                        my_row.append(int(open_interest.replace(",", "")))

                    my_table.append(my_row)

        return my_table

    def get_all_strike_urls(self, link):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            r1 = executor.map(self.get_strike_url, self.get_experation_urls(link))
            for r in r1:
                for item in r:

                    yield item

    def get_all_options_info(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            r1 = executor.map(
                self.info_getter,
                self.filter_generator(self.get_all_strike_urls(self.url)),
            )
            for r in r1:
                for item in r:
                    yield item
