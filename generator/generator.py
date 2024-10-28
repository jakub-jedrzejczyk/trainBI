import requests
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Manager, Lock
import sys
import random
from time import strptime as strptime
from faker import Faker
import pandas as pd
import os
import datetime
from dateutil.relativedelta import relativedelta


THREAD_COUNT = 16


def get_random_fault_description(id):
    fault_descriptions = [
        # brak pradu
        [
            "Przerwa w dostawie energii elektrycznej uniemożliwia dalszą podróż pociągu.",
            "Awaria w sieci energetycznej zatrzymała ruch kolejowy na tym odcinku.",
            "Pociąg zatrzymał się z powodu braku zasilania elektrycznego.",
            "Niespodziewany brak prądu zakłócił planowy przejazd.",
            "Problemy z infrastrukturą energetyczną spowodowały przestój w ruchu pociągów.",
            "Z powodu braku energii elektrycznej pociąg nie może kontynuować jazdy.",
            "Uszkodzenie sieci elektrycznej odcięło zasilanie na trasie pociągu.",
            "Zakłócenia w dostawie prądu opóźniły podróż pociągu na tym odcinku.",
            "Z powodu braku zasilania pociąg zatrzymał się na trasie.",
            "Awaria energetyczna doprowadziła do zatrzymania pociągów na tej linii."
        ],
        # uszkodzenie torow
        [
            'Deformacja torów zmusiła pociąg do zatrzymania się.',
            'Uszkodzenie torowiska wymusiło awaryjne wstrzymanie ruchu pociągów.',
            'Zniekształcenie torów sprawia, że dalsza podróż jest niemożliwa.',
            'Uszkodzenie odcinka toru stanowi zagrożenie dla przejeżdżających pociągów.',
            'Awaria na torowisku opóźnia ruch pociągów na tej trasie.',
            'Konieczność naprawy torów spowodowała przestój w ruchu.',
            'Niestabilność torowiska zmusiła pociąg do postoju.',
            'W wyniku uszkodzenia toru niezbędne jest wstrzymanie przejazdów.',
            'Deformacja torowiska skutkuje koniecznością interwencji służb technicznych.',
            'Usterka w strukturze torów uniemożliwia bezpieczną jazdę.'
        ],
        # uszkodzenie lokomotywy
        [
            'Awaria silnika uniemożliwia dalsze prowadzenie lokomotywy.',
            'Uszkodzenie mechaniczne w lokomotywie zatrzymało pociąg na trasie.',
            'Lokomotywa wymaga naprawy z powodu usterki systemu napędowego.',
            'Problemy techniczne z lokomotywą spowodowały zatrzymanie pociągu.',
            'Lokomotywa uległa awarii, przez co nie można kontynuować podróży.',
            'Awaria w układzie sterowania wymusiła postój pociągu.',
            'Lokomotywa potrzebuje interwencji techników z powodu usterki.',
            'Uszkodzenie w układzie hamulcowym lokomotywy wstrzymało ruch.',
            'Lokomotywa zatrzymała się z powodu problemów z elektroniką.',
            'Awaria w systemie napędowym uniemożliwia dalsze prowadzenie pociągu.',
        ],
        # uszkodzenie wagonu
        [
            'Problemy techniczne z jednym z wagonów wstrzymały przejazd.',
            'Awaria układu jezdnego wagonu wymaga natychmiastowej naprawy.',
            'Wagon uległ uszkodzeniu, co powoduje opóźnienie w ruchu pociągu.',
            'Zidentyfikowano problem z drzwiami w jednym z wagonów, zatrzymując pociąg.',
            'Nieszczelność układu klimatyzacji w wagonie powoduje konieczność interwencji.',
            'Awaria w wagonie sprawia, że podróż nie może być kontynuowana bezpiecznie.',
            'Usterka hamulca w jednym z wagonów wymaga natychmiastowej interwencji.',
            'Uszkodzenie oświetlenia w wagonie zatrzymało dalszy ruch pociągu.',
            'Problemy z zawieszeniem wagonu wstrzymały przejazd na tym odcinku.',
            'Awaria w systemie ogrzewania wagonu wymaga czasowego postoju.'
        ],
        # uszkodzenie sieci trakcyjnej
        [
            'Uszkodzenie przewodu trakcyjnego uniemożliwia dalszą jazdę pociągu.',
            'Awaria sieci trakcyjnej wymusiła zatrzymanie pociągu na trasie.',
            'Zerwany przewód trakcyjny powoduje opóźnienia w ruchu kolejowym.',
            'Problem z napięciem w sieci trakcyjnej zatrzymał przejazd pociągu.',
            'Awaria sieci trakcyjnej wymaga pilnej naprawy, by przywrócić ruch.',
            'Przeciążenie sieci trakcyjnej uniemożliwia dalszą jazdę pociągu.',
            'Uszkodzony przewód trakcyjny stanowi zagrożenie dla przejeżdżających składów.',
            'Problemy z siecią trakcyjną zatrzymały pociągi na tej linii.',
            'Awaria zasilania trakcyjnego wymaga interwencji techników.',
            'Problemy z napięciem w sieci trakcyjnej spowodowały zatrzymanie pociągu.'
        ],
        # uszkodzenie sygnalizacji
        [
            'Awaria systemu sygnalizacji uniemożliwia kontynuację podróży.',
            'Problemy z sygnalizacją spowodowały zatrzymanie ruchu pociągów.',
            'Zakłócenie w pracy sygnalizacji wymaga interwencji technicznej.',
            'Awaria sygnalizacji na trasie powoduje opóźnienia w ruchu kolejowym.',
            'Problemy z systemem świetlnym uniemożliwiają dalszą jazdę.',
            'Sygnalizacja wskazuje błędne informacje, co wymaga zatrzymania pociągu.',
            'Zakłócenia sygnalizacji powodują niepewność w kontynuacji jazdy.',
            'Błędne wskazania sygnalizacji wymuszają postój pociągu.',
            'Awaria świateł na sygnalizatorach zatrzymała ruch pociągów.',
            'Problemy z sygnalizacją wprowadzają zakłócenia w harmonogramie przejazdów.'
        ],
        # uszkodzenie semafora
        [
            'Uszkodzony semafor wymusił zatrzymanie pociągu na trasie.',
            'Awaria semafora powoduje zakłócenia w ruchu pociągów.',
            'Semafor nie działa poprawnie, co wymaga natychmiastowej naprawy.',
            'Problemy z działaniem semafora wymuszają postój pociągu.',
            'Usterka w systemie semaforów powoduje opóźnienia na linii.',
            'Semafor nieprawidłowo wskazuje sygnały, co zatrzymuje pociąg.',
            'Awaria mechanizmu semafora uniemożliwia dalszy ruch pociągu.',
            'Niesprawny semafor wymaga interwencji służb technicznych.',
            'Problemy z semaforem powodują konieczność postoju składu.',
            'Awaria semafora zatrzymuje pociągi do czasu usunięcia usterki.'
        ],
        # uszkodzenie rozjazdu
        [
            'Rozjazd jest uszkodzony, co uniemożliwia zmianę toru pociągu.',
            'Problemy z mechanizmem rozjazdu wymusiły zatrzymanie ruchu kolejowego.',
            'Awaria rozjazdu sprawia, że pociąg nie może kontynuować jazdy.',
            'Uszkodzenie rozjazdu wymaga naprawy, by przywrócić ruch na trasie.',
            'Blokada na rozjeździe zatrzymała pociągi na tym odcinku.',
            'Awaria rozjazdu uniemożliwia zmianę toru, co opóźnia ruch.',
            'Problemy techniczne na rozjeździe powodują przestój w ruchu.',
            'Usterka mechanizmu rozjazdu zatrzymała pociąg na trasie.',
            'Rozjazd nie działa poprawnie, co zmusza do postoju pociągu.',
            'Awaria w systemie rozjazdu powoduje opóźnienie w harmonogramie przejazdów.'
        ],
        # uszkodzenie peronu
        [
            'Uszkodzenie nawierzchni peronu opóźnia przyjazd pociągu.',
            'Problemy z konstrukcją peronu wymusiły czasowy przestój pociągów.',
            'Awaria peronu uniemożliwia bezpieczne wsiadanie i wysiadanie pasażerów.',
            'Uszkodzenie na peronie wymaga interwencji służb technicznych.',
            'Z powodu uszkodzeń peronu pociągi nie mogą zatrzymać się na stacji.',
            'Problem z peronem stanowi zagrożenie dla pasażerów i wymaga naprawy.',
            'Awaria nawierzchni peronu uniemożliwia bezpieczne zatrzymanie pociągu.',
            'Peron jest uszkodzony, co zatrzymuje ruch pociągów na tej stacji.',
            'Problemy konstrukcyjne peronu powodują przestój w ruchu kolejowym.',
            'Uszkodzenie peronu sprawia, że pociąg nie może obsłużyć tej stacji.'
        ],
        [
            'Problemy z infrastrukturą uniemożliwiają dalszy ruch pociągów.',
            'Uszkodzenie infrastruktury kolejowej zatrzymało przejazd pociągów.',
            'Awaria w infrastrukturze wymaga pilnej interwencji technicznej.',
            'Problemy z obiektami infrastruktury kolejowej opóźniają przejazd pociągu.',
            'Zniszczenie w infrastrukturze zatrzymało ruch na tej trasie.',
            'Awaria kluczowego elementu infrastruktury uniemożliwia kontynuację podróży.',
            'Uszkodzenie infrastruktury kolejowej zmusiło pociągi do postoju.',
            'Problemy z konstrukcją infrastruktury stanowią zagrożenie dla ruchu pociągów.',
            'Awaria elementu infrastruktury wymusza zatrzymanie pociągów na trasie.',
            'Problemy z obiektami kolejowymi wstrzymują przejazd pociągów na tej linii.'
        ]
    ]

    description_id = random.randint(0, len(fault_descriptions[id]) - 1)
    return fault_descriptions[id][description_id], description_id


def get_random_fault_type():
    # get a random fault type
    fault_types = ["Brak prądu", "Uszkodzenie torów", "Uszkodzenie lokomotywy", "Uszkodzenie wagonu", "Uszkodzenie sieci trakcyjnej", "Uszkodzenie sygnalizacji", "Uszkodzenie semafora", "Uszkodzenie rozjazdu", "Uszkodzenie peronu", "Uszkodzenie infrastruktury"]
    fault_id = random.randint(0, len(fault_types) - 1)
    fault_description, fault_description_id = get_random_fault_description(fault_id)
    return fault_types[fault_id], fault_description, fault_description_id


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


def get_city_from_stop(stop):
    # get the city from the stop
    # print(stop)
    return stop.split(",")[0].strip()


class ThreadedTrainDataCollection:
    def __init__(self):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.train_titles = []
        self.is_broken = []
        self.last_fault = []

    def add_train_data(self, train_title):
        toReturn = None
        with self.lock:
            if train_title in self.train_titles:
                return -1
            self.train_titles.append(train_title)
            self.is_broken.append(False)
            self.last_fault.append(datetime.datetime("1900-01-01"))
            toReturn = len(self.train_titles) - 1
        return toReturn

    def load_train_breaks_and_faults(self, breaks, faults):
        with self.lock:
            self.is_broken = breaks
            self.last_fault = faults

    def output_to_csv(self, filename):
        df = pd.DataFrame({
            "Train Title": self.train_titles,
            "Last Fault": self.last_fault,
            "Is Broken": [1 if is_broken else 0 for is_broken in self.is_broken]
        })

        df.index.name = "Index"
        df.to_csv(filename)

    def input_from_csv(self, filename):
        df = pd.read_csv(filename)
        self.train_titles = df["Train Title"].tolist()


class ThreadedStopDataCollection:
    def __init__(self):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.stop_data = []
        self.stop_cities = []
        self.stop_addresses = []
        self.platform_counts = []
        self.platform_track_counts = []
        self.fake = Faker(["pl_PL"])

    def add_stop_data(self, stop_data):
        toReturn = None
        with self.lock:
            stop = stop_data.strip()
            if stop == "":
                return -1
            if stop in self.stop_data:
                return self.stop_data.index(stop)
            self.stop_data.append(stop)
            self.stop_cities.append(get_city_from_stop(stop))
            address = self.fake.address().replace("\n", " ")
            # remove everything after the rightmost digit
            index = len(address) - 1
            while index >= 0 and not address[index].isdigit():
                index -= 1
            if index != -1:
                address = address[:index + 1]
            self.stop_addresses.append(address)
            platform_count = random.randint(1, 10)
            self.platform_counts.append(platform_count)
            self.platform_track_counts.append(platform_count * 2)
            toReturn = len(self.stop_data) - 1
        return toReturn

    def output_to_csv(self, filename):
        df = pd.DataFrame({
            "Stop Data": self.stop_data,
            "Stop City": self.stop_cities,
            "Stop Address": self.stop_addresses,
            "Platform Count": self.platform_counts,
            "Platform Track Count": self.platform_track_counts
        })

        df.index.name = "Index"
        df.to_csv(filename)

    def input_from_csv(self, filename):
        df = pd.read_csv(filename)
        self.stop_data = df["Stop Data"].tolist()
        # if "Stop City" is not in the csv file, generate it
        if "Stop City" not in df.columns:
            self.stop_cities = [get_city_from_stop(stop) for stop in self.stop_data]
        else:
            self.stop_cities = df["Stop City"].tolist()

        # if "Stop Address" is not in the csv file, generate it
        if "Stop Address" not in df.columns:
            # make addresses, remove newlines
            self.stop_addresses = [self.fake.address().replace("\n", " ") for stop in self.stop_data]
            for address in self.stop_addresses:
                # find index of rightmost digit
                index = len(address) - 1
                while index >= 0 and not address[index].isdigit():
                    index -= 1

                # remove everything after the rightmost digit
                if index != -1:
                    self.stop_addresses[self.stop_addresses.index(address)] = address[:index + 1]
        else:
            self.stop_addresses = df["Stop Address"].tolist()
        
        # if "Platform Count" is not in the csv file, generate it
        if "Platform Count" not in df.columns:
            self.platform_counts = [random.randint(1, 10) for stop in self.stop_data]
        else:
            self.platform_counts = df["Platform Count"].tolist()
        
        # if "Platform Track Count" is not in the csv file, generate it
        if "Platform Track Count" not in df.columns:
            self.platform_track_counts = [platform_count * 2 for platform_count in self.platform_counts]
        else:
            self.platform_track_counts = df["Platform Track Count"].tolist()


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

    def input_from_csv(self, filename):
        df = pd.read_csv(filename)
        self.section_data = [(row["Section Start"], row["Section End"]) for index, row in df.iterrows()]


class ThreadedDriverDataCollection:
    def __init__(self):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.driver_pesels = []
        self.driver_names = []
        self.driver_surnames = []
        self.driver_phone_numbers = []
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
            self.driver_phone_numbers.append(self.fake.phone_number())
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

    def input_from_csv(self, filename):
        df = pd.read_csv(filename)
        self.driver_pesels = df["Driver Pesel"].tolist()
        self.driver_names = df["Driver Name"].tolist()
        self.driver_surnames = df["Driver Surname"].tolist()
        if "Driver Phone Number" in df.columns:
            self.driver_phone_numbers = df["Driver Phone Number"].tolist()
        else:
            self.driver_phone_numbers = [self.fake.phone_number() for pesel in self.driver_pesels]


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

    def input_from_csv(self, filename):
        df = pd.read_csv(filename)
        self.relation_data = [(row["Driver Index"], row["Train Index"]) for index, row in df.iterrows()]


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
            planned_arrival = planned_arrival.replace("(", "")
            planned_arrival = planned_arrival.replace(")", "")
            planned_departure = planned_departure.replace("(", "")
            planned_departure = planned_departure.replace(")", "")
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

    def input_from_csv(self, filename):
        df = pd.read_csv(filename)
        self.realisation_data = [(row["Section Index"], row["Relation Index"]) for index, row in df.iterrows()]
        self.realisation_planned_times = [(row["Planned Arrival"], row["Planned Departure"]) for index, row in df.iterrows()]


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
        if train_index == -1 or "" in stops or None in stops or None in times:
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
        self.realisation_data_collection.output_to_csv(folder_name + "/realisation_data_base.csv")

    def input_from_csv(self, folder_name):
        self.train_data_collection.input_from_csv(folder_name + "/train_data.csv")
        self.stop_data_collection.input_from_csv(folder_name + "/stop_data.csv")
        self.section_data_collection.input_from_csv(folder_name + "/section_data.csv")
        self.driver_data_collection.input_from_csv(folder_name + "/driver_data.csv")
        self.relation_data_collection.input_from_csv(folder_name + "/relation_data.csv")
        self.realisation_data_collection.input_from_csv(folder_name + "/realisation_data_base.csv")


def random_number():
    # genearte random number between 0 and 1 (inclusive)
    return random.randint(0, 10000) / 10000


def rand_to_percentage_late(rand):
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
    if times is None:
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
        if title_container is None:
            return None, None, None
        hc = title_container.contents[1].contents[1].contents[2].contents[0].contents[0]
        if len(hc.contents) <= 1:
            return None, None, None
        vendor = hc.contents[1].get('title')
        number = hc.contents[2].strip()
        if (len(hc.contents) > 3):
            if type(hc.contents[3].contents[0]) is str:
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
    if stops is None:
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
        if stops_t is None or times_t is None:
            if titles_t is None:
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


def generate_single_realisation(data_object, current_day):
    next_day = data_object["next_day_to_calculate"]
    if next_day != current_day:
        # the train is not ready to be calculated yet,
        # return None
        return None

    # print("Calculating realisation for relation " + str(data_object["relation_index"]) + "...")
    # calculate the realisation
    planned_times = data_object["planned_times"]
    planned_times_r = []
    real_times = []
    faults = []
    is_broken = False
    last_fault = datetime.datetime.strptime("1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    overall_delay = 0
    fault_caused_delay = 0
    # number of times a day has passed during the relation
    days_delta = 0
    for i in range(len(planned_times)):
        planned_departure_from_start = time_to_minutes(planned_times[i][0])
        planned_arrival_at_end = time_to_minutes(planned_times[i][1])
        if planned_arrival_at_end is None or planned_departure_from_start is None:
            return None
        temp_date = current_day + datetime.timedelta(days=days_delta)
        planned_departure_datetime = datetime.datetime(temp_date.year, temp_date.month, temp_date.day, planned_departure_from_start // 60, planned_departure_from_start % 60)
        if planned_departure_from_start > planned_arrival_at_end:
            days_delta += 1
        temp_date = current_day + datetime.timedelta(days=days_delta)
        planned_arrival_datetime = datetime.datetime(temp_date.year, temp_date.month, temp_date.day, planned_arrival_at_end // 60, planned_arrival_at_end % 60)
        planned_times_r.append((planned_departure_datetime, planned_arrival_datetime))
        planned_duration = abs(planned_arrival_at_end - planned_departure_from_start)
        bonus_delay = rand_to_percentage_late(random_number())
        delay = int(planned_duration * bonus_delay / 100)
        if bonus_delay > 50:
            # 50 % chance of a fault
            if random_number() < 0.25:
                # fault
                fault, description, fault_id = get_random_fault_type()
                # print(fault)
                faults.append({
                    "reason": fault,
                    "description": description,
                    "type": fault + "-" + str(fault_id),
                    "index": i,
                })
                fault_caused_delay += random.randint(2, 14)
                is_broken = True
                last_fault = planned_arrival_datetime
        real_departure = planned_departure_datetime + datetime.timedelta(minutes=overall_delay)
        overall_delay += delay
        real_arrival = planned_arrival_datetime + datetime.timedelta(minutes=overall_delay)
        real_times.append((real_departure, real_arrival))
    
    # update the next day to calculate
    data_object["next_day_to_calculate"] = current_day + datetime.timedelta(days=days_delta + 1 + fault_caused_delay)

    # convert time to SQL-compatible yyyy-mm-dd hh:mm:ss format for export
    returnObject = {
        "relation_index": data_object["relation_index"],
        "section_indexes": data_object["section_indexes"],
        "planned_times": [(planned_time[0].strftime("%Y-%m-%d %H:%M:%S"), planned_time[1].strftime("%Y-%m-%d %H:%M:%S")) for planned_time in planned_times_r],
        "real_times": [(real_time[0].strftime("%Y-%m-%d %H:%M:%S"), real_time[1].strftime("%Y-%m-%d %H:%M:%S")) for real_time in real_times]
    }
    return returnObject, faults, is_broken, last_fault


def save_realisation_data(data, folder_name, file_name):
    df = pd.DataFrame({
        "Relation Index": [data_object["relation_index"] for data_object in data],
        "Section Indexes": [data_object["section_indexes"] for data_object in data],
        "Planned Departure": [data_object["planned_departure"] for data_object in data],
        "Planned Arrival": [data_object["planned_arrival"] for data_object in data],
        "Real Departure": [data_object["real_departure"] for data_object in data],
        "Real Arrival": [data_object["real_arrival"] for data_object in data]
    })

    df.index.name = "Index"
    df.to_csv(folder_name + "/" + file_name)


def save_fault_data(data, folder_name, file_name):
    df = pd.DataFrame({
        "Type": [fault["type"] for fault in data],
        "Description": [fault["description"] for fault in data],
        "Reason": [fault["reason"] for fault in data],
        "Realisation Index": [fault["realisation_index"] for fault in data],
    })

    df.index.name = "Index"
    df.to_csv(folder_name + "/" + file_name, sep=";")


def gender_string_from_pesel(pesel):
    return "Mężczyzna" if int(str(pesel)[9]) % 2 == 1 else "Kobieta"


def birthdate_from_pesel(pesel):
    year = year_from_pesel(str(pesel))
    month = int(str(pesel)[2:4])
    day = int(str(pesel)[4:6])
    if month > 20:
        month -= 20
    elif month > 40:
        month -= 40
    elif month > 60:
        month -= 60
    elif month > 80:
        month -= 80
    return datetime.datetime(year, month, day)


def monthly_summary_of_driver_hours(driver_data_collection, relation_data_collection, realisation_data, starting_date, folder_name, csv_number):
    # get the driver pesels
    driver_pesels = driver_data_collection.driver_pesels
    # get the relation data
    relation_data = relation_data_collection.relation_data
    # get the realisation data
    real_arrivals = [realisation["real_arrival"] for realisation in realisation_data]
    real_departures = [realisation["real_departure"] for realisation in realisation_data]
    relation_indexes = [realisation["relation_index"] for realisation in realisation_data]

    data = {}

    for i in range(len(relation_indexes)):
        relation_index = relation_indexes[i]
        # convert strings to datetime objects
        real_arrival = datetime.datetime.strptime(real_arrivals[i], "%Y-%m-%d %H:%M:%S")
        real_departure = datetime.datetime.strptime(real_departures[i], "%Y-%m-%d %H:%M:%S")

        year = real_arrival.year
        month = real_arrival.month

        key = str(year) + "-" + str(month)

        if key not in data:
            data[key] = {}

        # get the driver index
        driver_index = driver_pesels.index(relation_data[relation_index][0])
        if driver_index not in data[key]:
            data[key][driver_index] = {
                "day_hours": 0,
                "night_hours": 0
            }
        if real_arrival.hour >= 6 and real_arrival.hour < 22:
            data[key][driver_index]["day_hours"] += (datetime.timedelta(hours=real_arrival.hour, minutes=real_arrival.minute, seconds=real_arrival.second) - datetime.timedelta(hours=real_departure.hour, minutes=real_departure.minute, seconds=real_departure.second)).seconds / 3600
        else:
            data[key][driver_index]["night_hours"] += (datetime.timedelta(hours=real_arrival.hour, minutes=real_arrival.minute, seconds=real_arrival.second) - datetime.timedelta(hours=real_departure.hour, minutes=real_departure.minute, seconds=real_departure.second)).seconds / 3600

    # output the hours to a csv file
    for key in data:
        current_year, current_month = key.split("-")
        day_hours = []
        night_hours = []
        for driver_index in range(len(driver_pesels)):
            if driver_index not in data[key]:
                day_hours.append(0)
                night_hours.append(0)
                continue
            day_hours.append(data[key][driver_index]["day_hours"])
            night_hours.append(data[key][driver_index]["night_hours"])
        df = pd.DataFrame({
            "Driver ID": [i for i in range(len(driver_pesels))],
            "Driver Name": driver_data_collection.driver_names,
            "Driver Surname": driver_data_collection.driver_surnames,
            "Driver Pesel": driver_pesels,
            "Driver Gender": [gender_string_from_pesel(pesel) for pesel in driver_pesels],
            "Driver Birth Date": [birthdate_from_pesel(pesel).strftime("%Y-%m-%d") for pesel in driver_pesels],
            "Day Hours": [int(hours) for hours in day_hours],
            "Night Hours": [int(hours) for hours in night_hours],
            "Driver Phone Number": driver_data_collection.driver_phone_numbers
        })

        df.to_csv(folder_name + "/" + "driver_hours_" + csv_number + "_" + str(current_year) + "_" + str(current_month) + "_" + ".csv", index=False)


def save_real_realisation_data(data_count, data_orchestrator, starting_date, folder_name, train_faults, last_faults, data, faults):
    os.makedirs(folder_name + '/output/' + str(data_count), exist_ok=True)
    print("Reached " + str(data_count) + " data points, saving realisation data...")
    data_orchestrator.train_data_collection.load_train_breaks_and_faults(train_faults, last_faults)
    save_realisation_data(data, folder_name + '/output/' + str(data_count) + '/', "realisation_data.csv")
    print("Saving faults data...")
    save_fault_data(faults, folder_name + '/output/' + str(data_count), "fault_data.csv")
    print("Saving monthly summary of driver hours...")
    monthly_summary_of_driver_hours(data_orchestrator.driver_data_collection, data_orchestrator.relation_data_collection, data, starting_date, folder_name + '/output/' + str(data_count), "1")
    data_orchestrator.output_to_csv(folder_name + '/output/' + str(data_count))


def generate_real_realisations(threadpool, data_orchestrator, data_count_1, data_count_2, starting_date, folder_name):
    realisation_data = data_orchestrator.realisation_data_collection.realisation_data
    planned_times = data_orchestrator.realisation_data_collection.realisation_planned_times
    # consolidate the data by relation index found in realisation_data
    # find the maximum relation index
    max_relation_index = max([relation_index for section_index, relation_index in realisation_data])
    train_data = data_orchestrator.train_data_collection.train_titles
    relations = []
    for i in range(max_relation_index + 1):
        data_object = {
            "relation_index": i,
            "section_indexes": [],
            "planned_times": [],
            "next_day_to_calculate": starting_date
        }
        for k in range(len(realisation_data)):
            if realisation_data[k][1] == i:
                data_object["section_indexes"].append(realisation_data[k][0])
                data_object["planned_times"].append(planned_times[k])
        relations.append(data_object)
    # generate real data
    data = []
    faults = []
    train_faults = [False for i in range(len(train_data))]
    last_faults = [datetime.datetime(1900, 1, 1) for i in range(len(train_data))]
    current_day = starting_date
    point_1_saved = False
    while len(data) < data_count_2:
        print("Generating realisation data for day " + str(current_day) + "...")
        results = threadpool.starmap(generate_single_realisation, [(data_object, current_day) for data_object in relations])
        for result in results:
            if result is not None:
                # unwrap the results and append them to the data
                planned_times = result[0]["planned_times"]
                real_times = result[0]["real_times"]
                sections = result[0]["section_indexes"]
                relation_index = result[0]["relation_index"]
                for i in range(len(planned_times)):
                    data_object = {
                        "relation_index": relation_index,
                        "section_indexes": sections[i],
                        "planned_departure": planned_times[i][0],
                        "planned_arrival": planned_times[i][1],
                        "real_departure": real_times[i][0],
                        "real_arrival": real_times[i][1]
                    }
                    for fault in result[1]:
                        fault_object = None
                        if fault["index"] == i:
                            fault_object = {
                                "type": fault["type"],
                                "description": fault["description"],
                                "reason": fault["reason"]
                            }
                    data.append(data_object)
                    if fault_object is not None:
                        fault_object["realisation_index"] = len(data) - 1
                        faults.append(fault_object)
                trainIndex = data_orchestrator.relation_data_collection.relation_data[relation_index][1]
                if result[2] is True:
                    train_faults[trainIndex] = True
                    last_faults[trainIndex] = result[3]
                else:
                    train_faults[trainIndex] = False

        current_day += datetime.timedelta(days=1)
        if len(data) >= data_count_1 and not point_1_saved:
            save_real_realisation_data(data_count_1, data_orchestrator, starting_date, folder_name, train_faults, last_faults, data, faults)
            point_1_saved = True
    
    save_real_realisation_data(data_count_2, data_orchestrator, starting_date, folder_name, train_faults, last_faults, data, faults)


def main(find_vendors, folder_name, data_count_1=5000, data_count_2=10000):
    pool = ThreadPool(THREAD_COUNT)
    base_url = "https://www.vagonweb.cz/razeni/index.php?rok=2024&lang=pl"
    data_orchestrator = DataOrchestrator()
    if find_vendors is True:
        links = get_links(base_url)
        vendors, pageCounts = multithreaded_get_vendors_and_pagecounts(pool, links)
    else:
        vendors = ["PKPIC", "PREG"]
        pageCounts = [5, 11]
    print("Found a total of " + str(sum(pageCounts)) + " pages, provided by " + str(len(vendors)) + " vendors.")
    # check if provided folder already exists
    if os.path.exists("data/" + folder_name):
        print("Folder already exists, using it as cache...")
        data_orchestrator.input_from_csv("data/" + folder_name)
    else:
        os.makedirs("data/" + folder_name)
        os.makedirs("data/" + folder_name + "/output")
        multithreaded_get_vendors_data(pool, vendors, pageCounts, data_orchestrator)
        data_orchestrator.output_to_csv("data/" + folder_name)

    generate_real_realisations(pool, data_orchestrator, data_count_1, data_count_2, datetime.date(2021, 1, 1), "data/" + folder_name)


if __name__ == "__main__":
    random.seed(1234)
    if (len(sys.argv) == 5):
        main(sys.argv[1] == "1", sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
    else:
        print("Usage: python3 generator.py <find_vendors> <folder_name> <data_count_1> <data_count_2>")

