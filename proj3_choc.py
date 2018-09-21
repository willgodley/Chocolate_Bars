import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

# Connect to database
try:
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
except:
    print("Error occurred connecting to database")

def open_data_files():
    # Open CSV and JSON files
    try:
        bars = open(BARSCSV, 'r')
        bars_data = bars.read()
        bars.close()

        countries = open(COUNTRIESJSON, 'r')
        countries_data = countries.read()
        countries_dict = json.loads(countries_data)
        countries.close()

        return (bars_data, countries_dict)

    except:
        print("Error opening csv or json files.")

def make_counties_table():
    # Delete Country table if exists
    statement = "DROP TABLE IF EXISTS 'Countries'"
    cur.execute(statement)
    # Create new Countries table
    statement = """
        CREATE TABLE 'Countries' (
          'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
          'Alpha2' TEXT NOT NULL,
          'Alpha3' TEXT NOT NULL,
          'EnglishName' TEXT NOT NULL,
          'Region' TEXT NOT NULL,
          'Subregion' TEXT NOT NULL,
          'Population' INTEGER NOT NULL,
          'Area' REAL
        );
        """
    cur.execute(statement)

def make_bars_table():
    # Delete Bars table if exists
    statement = "DROP TABLE IF EXISTS 'Bars'"
    cur.execute(statement)
    # Create new Bars table
    statement = """
        CREATE TABLE 'Bars' (
          'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
          'Company' TEXT NOT NULL,
          'SpecificBeanBarName' TEXT NOT NULL,
          'REF' TEXT NOT NULL,
          'ReviewDate' TEXT NOT NULL,
          'CocoaPercent' REAL NOT NULL,
          'CompanyLocation' TEXT NOT NULL,
          'CompanyLocationId' INTEGER,
          'Rating' REAL NOT NULL,
          'BeanType' TEXT NOT NULL,
          'BroadBeanOrigin' TEXT NOT NULL,
          'BroadBeanOriginId' INTEGER
        );
    """
    cur.execute(statement)

def add_data(countries_and_bars):
    bars_data = countries_and_bars[0]
    countries_dict = countries_and_bars[1]

    countries_and_keys = {}
    count = 1
    for country in countries_dict:
        a = country['alpha2Code']
        b = country['alpha3Code']
        c = country['name']
        d = country['region']
        e = country['subregion']
        f = country['population']
        g = country['area']

        insertion1 = (count, a, b, c, d, e, f, g)
        statement = 'INSERT INTO "Countries" '
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion1)

        countries_and_keys[c] = count
        count += 1

    round_num = 0
    with open('flavors_of_cacao_cleaned.csv') as bars_csv:
        bars_data = csv.reader(bars_csv)
        for items in bars_data:
            if round_num == 0:
                round_num += 1
                continue
            a = items[0]
            b = items[1]
            c = items[2]
            d = items[3]
            e = items[4][:-1]
            f = items[5]
            g = 0
            k = 0
            h = items[6]
            i = items[7]
            j = items[8]

            for country in countries_and_keys.keys():
                if items[5] == country:
                    g = countries_and_keys[country]
                if items[8] == country:
                    k = countries_and_keys[country]

            insertion = (None, a, b, c, d, e, f, g, h, i, j, k)
            statement = 'INSERT INTO "Bars" '
            statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            cur.execute(statement, insertion)

            round_num += 1

    # Commit changes and close database connection
    conn.commit()
    conn.close()

def bar_command(command):
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Error occurred connecting to database")

    results= []
    bars_command = 'SELECT SpecificBeanBarName, Company, CompanyLocation, Rating, '
    bars_command += 'CocoaPercent, BroadBeanOrigin FROM Bars '

    if 'sellcountry' in command or 'sourcecountry' in command or 'sellregion' in command or 'sourceregion' in command:
        split_query = command.split()
        for option in split_query:
            if 'sellcountry' in option or 'sourcecountry' in option or 'sellregion' in option or 'sourceregion' in option:
                query = option.split('=')[0]
                country_or_region = option.split('=')[1]
        if query == 'sellcountry':
            bars_command += "JOIN Countries ON Countries.Id = Bars.CompanyLocationId WHERE Countries.Alpha2 = '{}' ".format(country_or_region)
        elif query == 'sourcecountry':
            bars_command += "JOIN Countries ON Countries.Id = Bars.BroadBeanOriginId WHERE Countries.Alpha2 = '{}' ".format(country_or_region)
        elif query == 'sellregion':
            bars_command += "JOIN Countries ON Countries.Id = Bars.CompanyLocationId WHERE Countries.Region = '{}' ".format(country_or_region)
        elif query == 'sourceregion':
            bars_command += "JOIN Countries ON Countries.Id = Bars.BroadBeanOriginId WHERE Countries.Region = '{}' ".format(country_or_region)

    if 'cocoa' in command:
        bars_command += 'ORDER BY CocoaPercent '
    else:
        bars_command += 'ORDER BY Rating '

    if 'top' in command:
        split_query = command.split()
        for option in split_query:
            if 'top' in option:
                num = option.split('=')[-1]
        bars_command += 'DESC LIMIT ' + str(num)
    elif 'bottom' in command:
        split_query = command.split()
        for option in split_query:
            if 'bottom' in option:
                num = option.split('=')[-1]
        bars_command += 'ASC LIMIT ' + str(num)
    else:
        bars_command += 'DESC LIMIT 10'

    cur.execute(bars_command)
    conn.commit()

    for info in cur:
        results.append(info)

    conn.close()
    return results

def companies_command(command):
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Error occurred connecting to database")

    results = []
    companies_command = 'SELECT Company, CompanyLocation, '

    if 'cocoa' in command:
        companies_command += 'AVG(CocoaPercent) '
    elif 'bars_sold' in command:
        companies_command += 'COUNT(Company) '
    else:
        companies_command += 'AVG(Rating) '

    companies_command += 'FROM Bars '

    if 'country' in command or 'region' in command:
        split_query = command.split()
        for option in split_query:
            if 'country' in option or 'region' in option:
                query = option.split('=')[0]
                country_or_region = option.split('=')[1]

        if query == 'country':
            companies_command += "JOIN Countries ON Countries.Id = Bars.CompanyLocationId WHERE Countries.Alpha2 = '{}' ".format(country_or_region)
        elif query == 'region':
            companies_command += "JOIN Countries ON Countries.Id = Bars.CompanyLocationId WHERE Countries.Region = '{}' ".format(country_or_region)

    companies_command += 'GROUP BY Company HAVING COUNT(Company) > 4 '

    if 'cocoa' in command:
        companies_command += 'ORDER BY AVG(CocoaPercent) '
    elif 'bars_sold' in command:
        companies_command += 'ORDER BY COUNT(Company) '
    else:
        companies_command += 'ORDER BY AVG(Rating) '

    if 'top' in command:
        split_query = command.split()
        for option in split_query:
            if 'top' in option:
                num = option.split('=')[-1]
        companies_command += 'DESC LIMIT ' + str(num)
    elif 'bottom' in command:
        split_query = command.split()
        for option in split_query:
            if 'bottom' in option:
                num = option.split('=')[-1]
        companies_command += 'ASC LIMIT ' + str(num)
    else:
        companies_command += 'DESC LIMIT 10'

    cur.execute(companies_command)
    conn.commit()

    for info in cur:
        results.append(info)

    conn.close()
    return results

def countries_command(command):
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Error occurred connecting to database")

    results = []
    countries_command = 'SELECT EnglishName, Region, '

    if 'cocoa' in command:
        countries_command += 'AVG(CocoaPercent) '
    elif 'bars_sold' in command:
        countries_command += 'COUNT(CompanyLocation) '
    else:
        countries_command += 'AVG(Rating) '

    countries_command += 'FROM Countries JOIN Bars ON Countries.Id = '

    if 'sources' in command:
        countries_command += 'Bars.BroadBeanOriginId '
    else:
        countries_command += 'Bars.CompanyLocationId '

    if 'region' in command:
        split_query = command.split()
        for option in split_query:
            if 'region' in option:
                region = option.split('=')[1]
        countries_command += "WHERE Countries.Region = '{}' ".format(region)

    countries_command += 'GROUP BY EnglishName HAVING COUNT(EnglishName) > 4 '

    if 'cocoa' in command:
        countries_command += 'ORDER BY AVG(CocoaPercent) '
    elif 'bars_sold' in command:
        countries_command += 'ORDER BY COUNT(CompanyLocation) '
    else:
        countries_command += 'ORDER BY AVG(Rating) '

    if 'top' in command:
        split_query = command.split()
        for option in split_query:
            if 'top' in option:
                num = option.split('=')[-1]
        countries_command += 'DESC LIMIT ' + str(num)
    elif 'bottom' in command:
        split_query = command.split()
        for option in split_query:
            if 'bottom' in option:
                num = option.split('=')[-1]
        countries_command += 'ASC LIMIT ' + str(num)
    else:
        countries_command += 'DESC LIMIT 10'

    cur.execute(countries_command)
    conn.commit()

    for info in cur:
        results.append(info)

    conn.close()
    return results

def regions_command(command):
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except:
        print("Error occurred connecting to database")

    results = []
    regions_command = 'SELECT Region, '

    if 'cocoa' in command:
        regions_command += 'AVG(CocoaPercent) '
    elif 'bars_sold' in command:
        regions_command += 'COUNT(CompanyLocation) '
    else:
        regions_command += 'AVG(Rating) '

    regions_command += 'FROM Countries JOIN Bars ON Countries.Id = '

    if 'sources' in command:
        regions_command += 'Bars.BroadBeanOriginId '
    else:
        regions_command += 'Bars.CompanyLocationId '

    regions_command += 'GROUP BY Region HAVING COUNT(Region) > 4 '

    if 'cocoa' in command:
        regions_command += 'ORDER BY AVG(CocoaPercent) '
    elif 'bars_sold' in command:
        regions_command += 'ORDER BY COUNT(CompanyLocation) '
    else:
        regions_command += 'ORDER BY AVG(Rating) '

    if 'top' in command:
        split_query = command.split()
        for option in split_query:
            if 'top' in option:
                num = option.split('=')[-1]
        regions_command += 'DESC LIMIT ' + str(num)
    elif 'bottom' in command:
        split_query = command.split()
        for option in split_query:
            if 'bottom' in option:
                num = option.split('=')[-1]
        regions_command += 'ASC LIMIT ' + str(num)
    else:
        regions_command += 'DESC LIMIT 10'

    cur.execute(regions_command)
    conn.commit()

    for info in cur:
        results.append(info)

    conn.close()
    return results

# Part 2: Implement logic to process user commands
def process_command(command):

    command_split = command.split()
    key = command_split[0]
    if key == 'bars':
        results = bar_command(command)
        if len(results) == 0:
            results = "no results"
    elif key == 'companies':
        results = companies_command(command)
        if len(results) == 0:
            results = "no results"
    elif key == 'countries':
        results = countries_command(command)
        if len(results) == 0:
            results = "no results"
    elif key == 'regions':
        results = regions_command(command)
        if len(results) == 0:
            results = "no results"
    else:
        results = "bad command"
    return results

def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    print("Type 'help' for list of commands.")
    while response != 'exit':
        response = input('Enter a command: ')

        if len(response.strip()) < 1:
            continue

        elif response == 'help':
            print(help_text)
            continue

        elif response == 'exit':
            print("Bye")
            continue

        else:
            data = process_command(response)
            if data == "bad command":
                print("Command not recognized: {}".format(response))
            elif data == "no results":
                print("No results from database from query: {}".format(response))
                print("Please try a different query")
            else:
                if len(data) == 0:
                    continue
                for single_response in data:
                    print_line = ""
                    for specific_item in single_response:
                        if type(specific_item) == float:
                            if specific_item > 5.0:
                                specific_item = str(int(specific_item)) + '%'
                            else:
                                specific_item = round(specific_item, 1)

                        if len(str(specific_item)) > 12:
                            specific_item = str(specific_item[:12])
                            specific_item = specific_item + '...'

                        if type(specific_item) != str or '%' in specific_item:
                            print_line += "{:<5}".format(str(specific_item))
                        else:
                            print_line += "{:<16}".format(str(specific_item))
                    print(print_line)
                print()

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    countries_and_bars = open_data_files()
    make_counties_table()
    make_bars_table()
    add_data(countries_and_bars)
    interactive_prompt()
