import requests
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Manager, Lock
import sys
import random
from time import strptime as strptime
from faker import Faker
import pandas as pd


THREAD_COUNT = 32


def year_from_pesel(pesel):
    # get the year from the pesel
    year = int(pesel[0:2])
    month = int(pesel[2:4])
    if month > 80:
        year += 1800
    elif month > 60:
        year += 2200
    elif month > 40:
        year += 2100
    elif month > 20:
        year += 2000
    else:
        year += 1900
    return year


class ThreadedTrainDataCollection:
    def __init__(self):
        self.manager = Manager()
        self.lock = self.manager.Lock() 
        self.train_titles = []

    def add_train_data(self, train_title):
        toReturn = None
        with self.lock:
            if train_title in self.train_titles:
                return -1
            self.train_titles.append(train_title)
            toReturn = len(self.train_titles) - 1
        return toReturn

    def output_to_csv(self, filename):
        df = pd.DataFrame({
            "Train Title": self.train_titles
        })

        df.index.name = "Index"
        df.to_csv(filename)

class ThreadedStopDataCollection:
    def __init__(self):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.stop_data = []

    def add_stop_data(self, stop_data):
        toReturn = None
        with self.lock:
            stop = stop_data.strip()
            if stop in self.stop_data:
                return self.stop_data.index(stop)
            self.stop_data.append(stop)
            toReturn = len(self.stop_data) - 1
        return toReturn

    def output_to_csv(self, filename):
        df = pd.DataFrame({
            "Stop Data": self.stop_data
        })

        df.index.name = "Index"
        df.to_csv(filename)


class ThreadedSectionDataCollection:
    def __init__(self, stop_data_collection):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.section_data = []
        self.stop_data_collection = stop_data_collection

    def add_section_data(self, section_start, section_end):
        toReturn = None
        with self.lock:
            section_start_index = self.stop_data_collection.add_stop_data(section_start)
            section_end_index = self.stop_data_collection.add_stop_data(section_end)
            section_data = (section_start_index, section_end_index)
            if section_data in self.section_data:
                return self.section_data.index(section_data)
            self.section_data.append(section_data)
            toReturn = len(self.section_data) - 1
        return toReturn

    def output_to_csv(self, filename):
        # Extract columns from the section data
        section_starts, section_ends = zip(*self.section_data)

        # Create the DataFrame
        df = pd.DataFrame({
            "Section Start": section_starts,
            "Section End": section_ends
        })

        df.index.name = "Index"
        df.to_csv(filename)


class ThreadedDriverDataCollection:
    def __init__(self):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.driver_pesels = []
        self.driver_names = []
        self.driver_surnames = []
        self.iteration = 1
        self.fake = Faker(["pl_PL"])
        self.fake_pesel = Faker(["pl_PL"])

    def driver_gender(self, pesel):
        # 0 if female, 1 if male
        return int(pesel[9]) % 2

    def add_driver(self):
        toReturn = None
        with self.lock:
            pesel = self.fake_pesel.unique.pesel()
            while pesel in self.driver_pesels or year_from_pesel(pesel) > 1999 or year_from_pesel(pesel) < 1950:
                pesel = self.fake_pesel.unique.pesel()
            self.driver_pesels.append(pesel)
            if self.driver_gender(pesel) == 0:
                name = self.fake.first_name_female()
                surname = self.fake.last_name_female()
            else:
                name = self.fake.first_name_male()
                surname = self.fake.last_name_male()
            self.driver_names.append(name)
            self.driver_surnames.append(surname)
            iteration = (self.iteration % 1000) + 1
            toReturn = pesel
        return toReturn

    def output_to_csv(self, filename):
        # output driver data and their indexes to a csv file
        df = pd.DataFrame({
            "Driver Pesel": self.driver_pesels,
            "Driver Name": self.driver_names,
            "Driver Surname": self.driver_surnames
        })
        df.index.name = "Index"
        df.to_csv(filename)


class ThreadedRelationDataCollection:
    def __init__(self, driver_data_collection, train_data_collection):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.relation_data = []
        self.driver_data_collection = driver_data_collection
        self.train_data_collection = train_data_collection

    def add_relation_data(self, driver_index, train_index):
        toReturn = None
        with self.lock:
            relation_data = (driver_index, train_index)
            if relation_data in self.relation_data:
                return self.relation_data.index(relation_data)
            self.relation_data.append(relation_data)
            toReturn = len(self.relation_data) - 1
        return toReturn

    def output_to_csv(self, filename):
        # Extract columns from the relation data
        driver_indexes, train_indexes = zip(*self.relation_data)

        # Create the DataFrame
        df = pd.DataFrame({
            "Driver Index": driver_indexes,
            "Train Index": train_indexes
        })

        df.index.name = "Index"
        df.to_csv(filename)


class ThreadedRealisationDataCollection:
    def __init__(self, section_data_collection, relation_data_collection):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.realisation_data = []
        self.realisation_planned_times = []
        self.section_data_collection = section_data_collection
        self.relation_data_collection = relation_data_collection

    def add_realisation_data(self, section_index, relation_index, planned_arrival, planned_departure):
        toReturn = None
        with self.lock:
            realisation_data = (section_index, relation_index)
            if realisation_data in self.realisation_data:
                return self.realisation_data.index(realisation_data)
            self.realisation_data.append(realisation_data)
            planned_arrival = planned_arrival.replace("(","")
            planned_arrival = planned_arrival.replace(")","")
            planned_departure = planned_departure.replace("(","")
            planned_departure = planned_departure.replace(")","")
            self.realisation_planned_times.append((planned_arrival, planned_departure))
            toReturn = len(self.realisation_data) - 1
        return toReturn

    def output_to_csv(self, filename):
        # Extract columns from the realisation data and times
        section_indexes, relation_indexes = zip(*self.realisation_data)
        planned_arrivals, planned_departures = zip(*self.realisation_planned_times)
    
        # Create the DataFrame
        df = pd.DataFrame({
            "Section Index": section_indexes,
            "Relation Index": relation_indexes,
            "Planned Arrival": planned_arrivals,
            "Planned Departure": planned_departures
        })
    
        # Set the index name and save to CSV
        df.index.name = "Index"
        df.to_csv(filename)


class DataOrchestrator:
    def __init__(self):
        self.train_data_collection = ThreadedTrainDataCollection()
        self.stop_data_collection = ThreadedStopDataCollection()
        self.section_data_collection = ThreadedSectionDataCollection(self.stop_data_collection)
        self.driver_data_collection = ThreadedDriverDataCollection()
        self.relation_data_collection = ThreadedRelationDataCollection(self.driver_data_collection, self.train_data_collection)
        self.realisation_data_collection = ThreadedRealisationDataCollection(self.section_data_collection, self.relation_data_collection)

    def add_train_data(self, train_title):
        return self.train_data_collection.add_train_data(train_title)

    def add_section_data(self, section_start, section_end):
        return self.section_data_collection.add_section_data(section_start, section_end)

    def add_driver(self):
        return self.driver_data_collection.add_driver()

    def add_relation_data(self, driver_index, train_index):
        return self.relation_data_collection.add_relation_data(driver_index, train_index)

    def add_realisation_data(self, section_index, relation_index, planned_arrival, planned_departure):
        return self.realisation_data_collection.add_realisation_data(section_index, relation_index, planned_arrival, planned_departure)

    def add_data(self, titles, stops, times):
        train_index = self.add_train_data(titles)
        if train_index == -1:
            return
        relation_index = self.add_relation_data(self.add_driver(), train_index)
        for i in range(len(stops) - 1):
            section_index = self.add_section_data(stops[i], stops[i + 1]) 
            self.add_realisation_data(section_index, relation_index, times[i], times[i + 1])

    def output_to_csv(self, folder_name):
        self.train_data_collection.output_to_csv(folder_name + "/train_data.csv")
        self.stop_data_collection.output_to_csv(folder_name + "/stop_data.csv")
        self.section_data_collection.output_to_csv(folder_name + "/section_data.csv")
        self.driver_data_collection.output_to_csv(folder_name + "/driver_data.csv")
        self.relation_data_collection.output_to_csv(folder_name + "/relation_data.csv")
        self.realisation_data_collection.output_to_csv(folder_name + "/realisation_data.csv")


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
        return None
    return h * 60 + m


def minuts_to_time(minutes):
    h = (minutes // 60) % 24
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def interpolate_time(start_time, end_time, count):
    start_minutes = time_to_minutes(start_time)
    end_minutes = time_to_minutes(end_time)
    if (start_minutes is None or end_minutes is None):
        return None
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

        if prevTime is None or nextTime is None:
            return None
        # interpolate between the two known times
        unknownCount = nextTime - prevTime
        interpolatedTimes = interpolate_time(times[prevTime], times[nextTime], unknownCount)
        if interpolatedTimes is None:
            return None
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
            time = time.replace(".", ":")
            if ("." in time):
                print(stop)
            times.append(time)
        elif check_if_even_weirder_time(stop):
            # stop has time in hh.mm format
            parts = stop.split(" ")
            stops.append(" ".join(parts[:-1]))
            time = parts[-1]
            print(time)
            time = time.replace(".", ":")
            print(time)
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
    else:
        return None, None

    stops, times = parse_stops_and_times(stops_with_times, v)
    times = interpolate_times(times, v)
    if times == None:
        return None, None
    return stops, times


def get_train_data(url, v=""):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    titles = soup.find_all("span", class_="velky15")
    titles = [title["title"].strip() for title in titles]
    if titles == []:
        # no title found, using secondary method
        title_container = soup.find(id="stred0")
        if title_container == None:
            return None, None, None
        hc = title_container.contents[1].contents[1].contents[2].contents[0].contents[0]
        vendor = hc.contents[1].get('title')
        number = hc.contents[2].strip()
        if (len(hc.contents) > 3):
            if type(hc.contents[3].contents[0]) == str:
                name = hc.contents[3].contents[0]
            else:
                return None, None, None
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
    if stops == None:
        return titles, None, None
    return titles, stops, times


def get_vendor_page_data(vendor, page, data_orchestrator):
    url = "https://www.vagonweb.cz/razeni/razeni.php?rok=2024&zeme=" + vendor + "&s=" + str(page)
    printv(vendor, "Getting page " + str(page) + "...")
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    train_table = soup.find_all("tr", class_="tr_razeni")
    for train in train_table:
        param = train['onclick'].split("'")[1]
        train_link = "https://www.vagonweb.cz/razeni/" + param
        titles_t, stops_t, times_t = get_train_data(train_link, vendor)
        if stops_t == None or times_t == None:
            if titles_t == None:
                printv(vendor, "Skipping train with no data.")
            else:
                printv(vendor, "Skipping train " + titles_t[0])
            continue
        data_orchestrator.add_data(titles_t[0], stops_t, times_t)
        printv(vendor, "Got data for train " + titles_t[0])
 

def multithreaded_get_vendors_data(pool, vendors, pageCounts, data_orchestrator):
    vendors_l = []
    pages_l = []
    for i in range(len(vendors)):
        pages_l.extend([(vendors[i], j) for j in range(1, pageCounts[i] + 1)])
        vendors_l.extend([vendors[i]] * pageCounts[i])

    results = pool.starmap(get_vendor_page_data, zip(vendors_l, pages_l, [data_orchestrator] * len(vendors_l)))
    return results


def main(data_count=5000):
    # get_vendor_data("ČD", 59)
    pool = ThreadPool(THREAD_COUNT)
    base_url = "https://www.vagonweb.cz/razeni/index.php?rok=2024&lang=pl"
    links = get_links(base_url)
    data_orchestrator = DataOrchestrator()
    vendors, pageCounts = multithreaded_get_vendors_and_pagecounts(pool, links)
    #vendors = ["PKPIC", "PREG"]
    #pageCounts = [5, 11]
    print("Found a total of " + str(sum(pageCounts)) + " pages, provided by " + str(len(vendors)) + " vendors.")
    multithreaded_get_vendors_data(pool, vendors, pageCounts, data_orchestrator)
    data_orchestrator.output_to_csv("data1")


if __name__ == "__main__":
    random.seed(1234)
    if (len(sys.argv) > 1):
        main(sys.argv[1])
    else:
        main()

