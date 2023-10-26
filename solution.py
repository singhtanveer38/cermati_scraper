from bs4 import BeautifulSoup
import requests
import json
import multiprocessing
import pandas as pd

# function get request to the link and saving the result
def get_request(url, filename):
    r = requests.get(url)

    soup = BeautifulSoup(r.content, "html.parser")

    content = []
    for i in soup.find_all("script", attrs={"id": "initials", "type": "application/json"}):
        content.append(i.contents)


    with open(filename, "w") as f:
        f.write(content[0][0])

#function to parse the json file
def json_parser(filename):
    with open(filename, "r") as f:
        data = json.load(f)

    return data["smartRecruiterResult"]

# function to extract data
def data_extract(link, q):
    job = requests.get(link).json()

    formated_data = {"department_name": job["department"]["label"],
                   "title": job["name"].split("-")[0],
                   "location": job["location"]["city"] + ", " + job["location"]["country"],
                    "job_type": job["typeOfEmployment"]["label"]
                     }

    descSoup = BeautifulSoup(job["jobAd"]["sections"]["jobDescription"]["text"], "html.parser")
    dataList = []
    for data in descSoup.stripped_strings:
        dataList.append(data)

    formated_data["description"] = " ".join(dataList)

    qualSoup = BeautifulSoup(job["jobAd"]["sections"]["qualifications"]["text"], "html.parser")
    dataList = []
    for data in descSoup.stripped_strings:
        dataList.append(data)

    formated_data["qualification"] = " ".join(dataList)

    q.put(formated_data)

url = "https://www.cermati.com/karir"
filename = "raw_data.json"
output = "solution.json"

get_request(url, filename)
data = json_parser(filename)

print("data collected")

# collecting all jobs links
links = []
for i in data["all"]["content"]:
    links.append(i["ref"])

# parallel execution of request to collect the data
q = multiprocessing.Queue()
processes = []
for i in links:
    p = multiprocessing.Process(target=data_extract, args=(i, q))
    p.start()

print("scrape completed")


# saving results into a json file
jobs = {"department_name": [], "title": [], "location": [], "job_type": [], "description": [], "qualification": []}
for i in range(len(links)):
    process_out = q.get()
    for i in jobs:
        jobs[i].append(process_out[i])

df = pd.DataFrame(jobs)
grouped = df.groupby(by="department_name")
jobs_nested = {}

for department_name, group in grouped:
    jobs_nested[department_name] = group[['title', 'location', "job_type", "description", "qualification"]].to_dict(orient='records')

jobs_nested_str = json.dumps(jobs_nested)

with open(output, "w") as f:
    f.write(jobs_nested_str)
