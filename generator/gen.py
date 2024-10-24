import requests
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool
import sys


THREAD_COUNT = 4


def data_count_to_vendor_count(data_count, found_vendor_count=20):
    min_vendors = 5
    max_vendors = found_vendor_count
    vendor_count = int((float(data_count) / 32767.0) + min_vendors)
    return min(max(vendor_count, min_vendors), max_vendors)


def get_links(url):
    print("Getting all possible links from the main page...")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # get all links with "razeni.php" but without "kategorie" in href

    links = soup.find_all("a", href=True)

    toReturn = []

    for link in links:
        if "razeni.php" in link["href"] and "kategorie" not in link["href"] and "zeme" in link["href"]:
            #print(link["href"])
            toReturn.append("https://www.vagonweb.cz/razeni/" + link["href"])

    print("Found " + str(len(toReturn)) + " links.")
    return toReturn


def printv(v, string):
    print("[" + v + "] " + str(string))


def get_vendors_and_pagecounts(url):
    # get value of parameter "zeme" from url,
    # it is the name of the vendor
    print("Getting vendors and page counts from " + url)
    v = url.split("zeme=")[1].split("&")[0]
    printv(v, "Getting data...")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    # find all links with the url containing ";s"
    possibleCounts = soup.find_all("a", href=True)
    possibleCounts = [link["href"] for link in possibleCounts if "s=" in link["href"]]
    nonDuplicates = []
    for link in possibleCounts:
        if link not in nonDuplicates:
            nonDuplicates.append(link)

    pageCount = len(nonDuplicates) + 1
    printv(v, "Found " + str(pageCount) + " pages.")
    return v, pageCount


def multithreaded_get_vendors_and_pagecounts(urls):
    pool = ThreadPool(THREAD_COUNT)
    results = pool.map(get_vendors_and_pagecounts, urls)
    vendors = [result[0] for result in results]
    pageCounts = [result[1] for result in results]
    return vendors, pageCounts


def get_vendor_page_data(vendor, page):
    url = "https://www.vagonweb.cz/razeni/razeni.php?rok=2024&zeme=" + vendor + "&s=" + str(page)
    # print(url)
    printv(vendor, "Getting page " + str(page) + "...")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    # print(soup.prettify())
    train_table = soup.find_all("tr", class_="tr_razeni")
    train_links = []
    for train in train_table:
        param = train['onclick'].split("'")[1]
        train_links.append("https://www.vagonweb.cz/razeni/" + param)

    print(train_links[0])


def main(data_count=5000):
    base_url = "https://www.vagonweb.cz/razeni/index.php?rok=2024&lang=pl"
    links = get_links(base_url)
    links = links[:4]
    vendors, pageCounts = multithreaded_get_vendors_and_pagecounts(links)
    print("Found a total of " + str(sum(pageCounts)) + " pages, provided by " + str(len(vendors)) + " vendors.")
    get_vendor_page_data(vendors[0], 1)


if __name__ == "__main__":
    if (len(sys.argv) > 1):
        main(sys.argv[1])
    else:
        main()
