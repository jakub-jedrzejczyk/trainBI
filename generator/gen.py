import requests
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool
import sys
import random
from time import strptime as strptime


THREAD_COUNT = 8


def random_number():
    # genearte random number between 0 and 1 (inclusive)
    return random.randint(0, 10000) / 10000


def rand_to_minutes_late(rand):
    # first function:
    a = 500
    b = 0
    c = -5
    # second function:
    d = 16
    f = -40
    h = -1
    g = -1
    i = -1
    # third function:
    j = -1
    k = 2
    ll = -0.9
    m = -1
    n = 320

    if rand <= 0.1:
        return a * pow(rand + b, 2) + c
    elif rand <= 0.6:
        return 0
    elif rand <= 0.95:
        return i * d * pow(rand + h, g) + f
    else:
        return j * k * pow(rand + ll, m) + n


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


def multithreaded_get_vendors_and_pagecounts(pool, urls):
    results = pool.map(get_vendors_and_pagecounts, urls)
    vendors = [result[0] for result in results]
    pageCounts = [result[1] for result in results]
    return vendors, pageCounts


def time_to_minutes(time_str):
    try:
        h, m = map(int, time_str.split(':'))
    except ValueError:
        print(time_str)
        h = 0
        m = 0
    return h * 60 + m


def minuts_to_time(minutes):
    h = (minutes // 60) % 24
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def interpolate_time(start_time, end_time, count):
    start_minutes = time_to_minutes(start_time)
    end_minutes = time_to_minutes(end_time)
    if (end_minutes < start_minutes):
        end_minutes += 24 * 60
    minutes = [start_minutes + (end_minutes - start_minutes) * i / count for i in range(1, count)]
    return [minuts_to_time(int(minute)) for minute in minutes]


def interpolate_times(times, v=""):
    iteration = 0
    while None in times:
        if (iteration > 100):
            printv(v, "Interpolation reached 100 iterations, check the data.")
        # find the first None
        firstNone = times.index(None)
        # find the previous time
        prevTime = None
        for i in range(firstNone, -1, -1):
            if times[i] is not None:
                prevTime = i
                break
        # find the next time
        nextTime = None
        for i in range(firstNone, len(times)):
            if times[i] is not None:
                nextTime = i
                break

        # interpolate between the two known times
        unknownCount = nextTime - prevTime
        interpolatedTimes = interpolate_time(times[prevTime], times[nextTime], unknownCount)
        times[prevTime + 1:nextTime] = interpolatedTimes

        iteration += 1

    return times


def check_if_unusal_time(stop):
    # check if stop has time in (hh.mm) format
    if "(" not in stop and ")" not in stop:
        return False
    if "." not in stop:
        return False

    left_p = stop.rindex("(")
    right_p = stop.rindex(")")
    dot = stop.rindex(".")

    if left_p < dot and dot < right_p:
        if (right_p - dot) == 3 and (dot - left_p) == 3:
            return True

    return False


def check_if_even_weirder_time(stop):
    # check if stop has time in hh.mm format
    if "." not in stop:
        return False

    dot = stop.rindex(".")

    # check if there are two digits before and after the dot
    if (stop[dot - 2].isdigit() and stop[dot - 1].isdigit() and
            stop[dot + 1].isdigit() and stop[dot + 2].isdigit()):
        return True

    return False


def parse_stops_and_times(stops_with_times, v=""):
    stops = []
    times = []
    for stop in stops_with_times:
        if (stop == ""):
            continue

        if ":" in stop:
            # stop has time, need to separate the rightmost
            # part of the string

            parts = stop.split(" ")
            stops.append(" ".join(parts[:-1]))
            time = parts[-1]
            if ("-" in time):
                # time range given, take the first one
                time = time.split("-")[0]
            elif ("/" in time):
                # strip out parentheses
                if ("(" in time):
                    time = time[1:]
                if (")" in time):
                    time = time[:-1]
                # multiple times given, take the first one
                time = time.split("/")[0]
            # print time if not hh:mm for debug
            times.append(time)
        elif check_if_unusal_time(stop):
            # stop has time in (hh.mm) format
            parts = stop.split(" ")
            stops.append(" ".join(parts[:-1]))
            time = parts[-1]
            # strip out parentheses
            if ("(" in time):
                time = time[1:]
            if (")" in time):
                time = time[:-1]
            time.replace(".", ":")
            if ("." in time):
                print(stop)
            times.append(time)
        elif check_if_even_weirder_time(stop):
            # stop has time in hh.mm format
            parts = stop.split(" ")
            stops.append(" ".join(parts[:-1]))
            time = parts[-1]
            time = time.replace(".", ":")
            times.append(time)
        else:
            # stop does not have time
            stops.append(stop)
            times.append(None)

    return stops, times


def stops_and_times_from_route(route, v=""):
    # replace dash ("–") with hyphen ("-")
    route = route.replace("–", "-")
    if (" - " in route and ", " in route):
        # split by both separators
        stops_with_times = route.split(", ")
        stops_with_times = [stop.split(" - ") for stop in stops_with_times]
    elif (" - " in route):
        stops_with_times = route.split(" - ")
    elif (", ") in route:
        stops_with_times = route.split(", ")

    stops, times = parse_stops_and_times(stops_with_times, v)
    times = interpolate_times(times, v)
    return stops, times


def get_train_data(url, v=""):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    titles = soup.find_all("span", class_="velky15")
    titles = [title["title"].strip() for title in titles]
    if titles == []:
        # no title found, using secondary method
        title_container = soup.find(id="stred0")
        hc = title_container.contents[1].contents[1].contents[2].contents[0].contents[0]
        vendor = hc.contents[1].get('title')
        number = hc.contents[2].strip()
        if (len(hc.contents) > 3):
            name = hc.contents[3].contents[0]
            title = vendor + " " + number + " " + name
        else:
            title = vendor + " " + number
        titles = [title]
    titles.sort()
    route = soup.find("div", class_="trasa jizdnirad2")
    route = route.contents[1].strip()
    # some times may be None, sometimes mulitple in a row,
    # interpolation needed between the given times
    stops, times = stops_and_times_from_route(route)
    return titles, stops, times


def get_vendor_page_data(vendor, page):
    if vendor == "IRC" or vendor == "SJ":
        return [], [], []

    url = "https://www.vagonweb.cz/razeni/razeni.php?rok=2024&zeme=" + vendor + "&s=" + str(page)
    printv(vendor, "Getting page " + str(page) + "...")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    train_table = soup.find_all("tr", class_="tr_razeni")
    titles = []
    stops = []
    times = []
    for train in train_table:
        param = train['onclick'].split("'")[1]
        train_link = "https://www.vagonweb.cz/razeni/" + param
        titles_t, stops_t, times_t = get_train_data(train_link, vendor)
        printv(vendor, "Got data for train " + titles_t[0])
        titles.append(titles_t)
        stops.append(stops_t)
        times.append(times_t)
    return titles, stops, times


def get_vendor_data(vendor, pageCount):
    printv(vendor, "Getting data for " + str(pageCount) + " pages...")
    titles = []
    stops = []
    times = []
    for page in range(1, pageCount):
        titles_t, stops_t, times_t = get_vendor_page_data(vendor, page)
        titles.extend(titles_t)
        stops.extend(stops_t)
        times.extend(times_t)
    return titles, stops, times


def multithreaded_get_vendors_data(pool, vendors, pageCounts):
    results = pool.starmap(get_vendor_data, zip(vendors, pageCounts))
    return results


def main(data_count=5000):
    # print(check_if_even_weirder_time("Praha hl.n. 06.00"))
    pool = ThreadPool(THREAD_COUNT)
    base_url = "https://www.vagonweb.cz/razeni/index.php?rok=2024&lang=pl"
    links = get_links(base_url)
    #links = links[:THREAD_COUNT]
    vendors, pageCounts = multithreaded_get_vendors_and_pagecounts(pool, links)
    print("Found a total of " + str(sum(pageCounts)) + " pages, provided by " + str(len(vendors)) + " vendors.")
    results = multithreaded_get_vendors_data(pool, vendors, pageCounts)


if __name__ == "__main__":
    random.seed(1234)
    if (len(sys.argv) > 1):
        main(sys.argv[1])
    else:
        main()
