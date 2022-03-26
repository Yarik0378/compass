import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import threading
from threading import Thread

lock = threading.Lock()
threads = []
threads_number = 12

class CompassScraper:
    df2 = pd.DataFrame()
    result = []
    all_result = []

    def __init__(self, i=None, j=None):
        self.count = 0
        self.i = i
        self.j = j
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0"
                          ".4758.82 Safari/537.36",
            'Cookie': "ajs_anonymous_id=%22738e2ded-c5f8-4924-9737-50c1c2beb0db%22; fingerprint=88634e12-d077-42c6-9a97"
                      "-b2336aafce5f; _ga=GA1.2.1065262557.1648057172; _gid=GA1.2.855362743.1648057172; authentication_"
                      "token=WyI2MWYzYmZlNmU0N2VmYzAwMDE4OGU0OTAiLCJTRVM6ZXlKelpYTWlPaUprTURGbU1qUm1NaTFqTkdOakxUUTVOMk"
                      "V0WVRnd01pMHdPR1EwWmpkak9UY3hNR1VpZlEiXQ.MjAyMi0wMy0yNFQxMToxMDozN1o.17Urn8v9_LGxDzyZWRKFKJSUBqE"
                      "; remember_token=WyI2MWYzYmZlNmU0N2VmYzAwMDE4OGU0OTAiLCJTRVM6ZXlKelpYTWlPaUprTURGbU1qUm1NaTFqTkd"
                      "OakxUUTVOMkV0WVRnd01pMHdPR1EwWmpkak9UY3hNR1VpZlEiXQ.MjAyMi0wMy0yNFQxMToxMDozN1o.17Urn8v9_LGxDzyZ"
                      "WRKFKJSUBqE; ajs_user_id=%2261f3bfe6e47efc000188e490%22; _gat=1; _hp2_ses_props.1748376918=%7B%2"
                      "2ts%22%3A1648215498195%2C%22d%22%3A%22www.compass.com%22%2C%22h%22%3A%22%2Ffor-rent%2Fnew-york-n"
                      "y%2F%22%7D; _dd_s=rum=1&id=878ded82-3b7e-4993-bd7f-b39a4050da67&created=1648215497727&expire=164"
                      "8216437809; clickstream_id=94a47706-5a16-43cb-98c7-b145f175ce02; _hp2_id.1748376918=%7B%22userId"
                      "%22%3A%22934771499553832%22%2C%22pageviewId%22%3A%225615160335882644%22%2C%22sessionId%22%3A%221"
                      "105259486375072%22%2C%22identity%22%3A%2261f3bfe6e47efc000188e490%22%2C%22trackerVersion%22%3A%2"
                      "24.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D"
        }
        self.json_data = {"searchResultId": "c74fc983-544e-4c6e-9b0d-f6e57c732ba8",
                          "rawLolSearchQuery": {"listingTypes": [0],
                                                "rentalStatuses": [7, 5],
                                                "num": 41,
                                                "sortOrder": 115,
                                                "start": 82,
                                                "locationIds": [21429],
                                                "facetFieldNames": ["contributingDatasetList", "compassListingTypes",
                                                                    "comingSoon"]},
                          "width": 745,
                          "height": 309,
                          "viewport": {"northeast": {"lat": 41.0097836, "lng": -73.0710237},
                                       "southwest": {"lat": 40.368345, "lng": -75.1144807}},
                          "purpose": "search"}

    def save_data(self, result):
        df_dict = pd.DataFrame(result)
        CompassScraper.df2 = pd.concat([CompassScraper.df2, df_dict], ignore_index=True, sort=False)

    def save_csv(self):
        CompassScraper.df2.to_csv('output.csv')

    def send_requests(self, url):
        response = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup

    # def send_requests_two(self, id):
    #     headers = {
    #         'content-type': 'application/json',
    #         'Content-Length': '41',
    #         'x-compass-response-filter': '{ listings { detailedInfo { keyDetails } } }',
    #         'Host': 'www.compass.com'
    #
    #     }
    #     payload = {
    #         "listingIdSHAs": [f"{id}"]
    #     }
    #     json_data_two = requests.post('https://www.compass.com/api/v3/listings/load',
    #                                   json=payload, headers=headers).json()
    #     return json_data_two

    def scrape_link_home(self):
        start = 0
        original_url = 'https://www.compass.com/for-rent/jersey-city-nj/'
        while True:
            if start == 0:
                url = original_url
            else:
                url = f"{original_url}start={start}"
            r = requests.post(url, json=self.json_data, headers=self.headers).text
            json_all_list = json.loads(r)
            json_list_link = json.loads(r)['lolResults']['data']
            for link in json_list_link:
                # link = 'https://www.compass.com' + link['listing']['navigationPageLink']
                link['listing']['name'] = json_all_list['geospatial']['locations'][0]['name']
                self.result.append(link['listing'])
            if not json_list_link:
                break
            print(url)
            start += 41
            # if start == 41:
            #     url = f"https://www.compass.com/for-rent/jersey-city-nj/start={start}"


    def scrape_data_from_link(self):

        for json_data in CompassScraper.result[self.i:self.j]:
            link_for_requests = 'https://www.compass.com' + json_data['navigationPageLink']
            soup = self.send_requests(link_for_requests)
            # json_data_two = self.send_requests_two(json_data["listingIdSHA"])
            try:
                unit = json_data['location']['unitNumber']
            except:
                unit = ''
            try:
                bedroom = json_data['size']['bedrooms']
            except:
                bedroom = '0'
            try:
                bathroom = json_data['size']['bathrooms']
            except:
                bathroom = '0'
            try:
                price = json_data['price']['lastKnown']
            except:
                price = 'ERROR'

            net_price = ''

            try:
                sqft = json_data['size']['squareFeet']
            except:
                sqft = ''
            try:
                floor_ = soup.select('span.building-info__BuildingInfoLineItem-sc-85jvb8-1')
                floor = ''
                for x in floor_:
                    if 'Floor' in x.text:
                        floor = x.select_one('strong').text.replace('-', '')
            except:
                floor = 'ERROR'
            try:
                description = re.search('"description":"(.*?)",', str(soup)).group(1).replace(r'\u002F', '/')
            except:
                description = 'ERROR'
            try:
                amenity = ''
                amenity_ = json_data['detailedInfo']['amenities']
                for li in amenity_:
                    amenity += li + '; '
            except:
                amenity = ''
            try:
                image_sourse = ''
                for img in soup.select('span.cx-react-button-textContent'):
                    if 'Floor Plan' == img.text:
                        image_sourse = json_data['media'][-1]['originalUrl']
            except:
                image_sourse = 'ERROR'

            try:
                street_number = json_data['location']['streetNumber']
            except:
                street_number = ' '
            try:
                street = json_data['location']['street']
            except:
                street = ' '
            try:
                street_type = json_data['location']['streetType']
            except:
                street_type = ' '
            try:
                borough = json_data['location']['city']
            except:
                borough = ' '
            try:
                state = json_data['location']['state']
            except:
                state = ' '
            try:
                zip_code = json_data['location']['zipCode']
            except:
                zip_code = ' '
            try:
                address = f"{street_number} {street} {street_type}, {borough}, {state} {zip_code}"
            except:
                address = ' '
            try:
                building_name = f"{street_number} {street} {street_type}"
            except:
                building_name = ' '
            try:
                floor_plan = building_name + '_' + unit
            except:
                floor_plan = ' '

            try:
                city = json_data['name']
            except:
                city = ' '
            try:
                neighborhood = json_data['location']['neighborhood']
            except:
                neighborhood = ''
                try:
                    neighborhood_ = soup.select('li')
                    for n_ in neighborhood_:
                        if 'Section' in n_.text:
                            neighborhood = n_.select_one('span').text
                except:
                    neighborhood = ''
            try:
                available = ' '
                status = ' '
                available_ = soup.select('.data-table__TableStyled-ibnf7p-0 tr ')
                for x in available_:
                    if 'Available Date' in x.text:
                        available = x.select_one('td').text.replace('-', '')
                    if 'Compass Type' in x.text:
                        status = x.select_one('td').text
            except:
                available = 'ERROR'
                status = 'ERROR'

            created_at = datetime.now().strftime('%d-%m-%Y')
            update_ut = datetime.now().strftime('%d-%m-%Y')
            if available == '':
                data_present = 0
            else:
                data_present = 1
            lock.acquire()
            CompassScraper.all_result.append({
                "Source": link_for_requests,
                "Unit": unit,
                "Bedroom": bedroom,
                "Bathroom": bathroom,
                "Gross Price": price,
                "Net price": net_price,
                "Sqft": sqft,
                "Floor": floor,
                "Image source": image_sourse,
                "Address": address,
                "Borough": borough,
                "City": city,
                "State": state,
                "Building Name": building_name,
                "Neighborhood": neighborhood,
                "Status": status,
                "Amenity": amenity,
                "Description": description,
                "Availability": available,
                "Floor_plan": floor_plan,
                "Date_present": data_present,
                "Created At": created_at,
                "Updated At": update_ut,
                "Closing Date": ''

            })
            # print('==============================')
            # print(f"{count + self.i} / {self.j}")
            # print('LINK: ', link_for_requests)

            self.count += 1
            var = ''
            for y in range(0, threads_number):
                var += ' -> THREAD ' + str(y) + ' count = ' + str(threads[y].cs.count) + ' all = ' + str(threads[y].j -
                                                                                                         threads[y].i)
            print(var)

            lock.release()


class Thread(threading.Thread):
    def __init__(self, i, j):
        threading.Thread.__init__(self)
        self.i = i
        self.j = j
        self.cs = CompassScraper(self.i, self.j)
    def run(self):
        self.cs.scrape_data_from_link()


cs = CompassScraper()
cs.scrape_link_home()
len_a = len(cs.result)


for x in range(0, threads_number):
    t = Thread(int(len_a/threads_number * x), int(len_a/threads_number * x + len_a / threads_number))
    print(int(len_a / threads_number * x), int(len_a / threads_number * x + len_a / threads_number))
    threads.append(t)
    t.start()
for t in threads:
    t.join()
cs.save_data(CompassScraper.all_result)
cs.save_csv()
