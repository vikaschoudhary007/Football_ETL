def get_wikipedia_page(url):
    import requests
    print("getting wikipedia page...", url)

    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status() # check if request was successful or not
        return res.text

    except requests.RequestException as e:
        print(f"Error : {e}")

def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find_all("table", {"class": "wikitable sortable sticky-header"})[0]
    table_rows = table.find_all('tr')

    return table_rows


def extract_wikipedia_data(**kwargs):
    import pandas as pd
    url = kwargs["url"]
    html = get_wikipedia_page(url)
    data = get_wikipedia_data(html)

    output = []

    for i in range(1, len(data)):
        tds = data[i].find_all('td')
        values = {
            'rank': i,
            'stadium': tds[0].text,
            'capacity': tds[1].text,
            'region': tds[2].text,
            'country': tds[3].text,
            'city': tds[4].text,
            'images': tds[5].find('img').get('src').split('//')[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': tds[6].text
        }
        output.append(values)

    data_df = pd.DataFrame(output)
    data_df.to_csv("data/output.csv", index=False)
    return output
