#Importing packages
import json
import requests
import time
import pandas as pd
import pyodbc

#DB Connection
def dbconnection():
    conn=pyodbc.connect(Trusted_Connected="Yes", Driver={'ODBC Driver 17 for SQL Server'}, Server=r"DESKTOP-57LQFIF\SQLEXPRESS", Database="projects")
    cursor=conn.cursor()
    return cursor
#Function to define scraper
def scraper():
    # Create a list to store all project details
    projects_data = []
    url="https://registry.goldstandard.org/projects?q=&page=1"
    for page in range(1, 140):
        Url = f"https://public-api.goldstandard.org/projects?query=&page={page}&size=25&sortColumn=&sortDirection="
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
            'origin': 'https://registry.goldstandard.org',
            'priority': 'u=1, i',
            'referer': 'https://registry.goldstandard.org/',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        retries = 3
        for attempt in range(retries):
            try:
                response = requests.get(Url, headers=headers)
                response.raise_for_status()  # Raise an error for bad responses
                data = response.json()  # Decode the JSON
                if len(data) != 0:
                    for item in data: 
                        project_name = item.get('name', 'N/A')
                        name = "".join(c if c.isalnum() or c.isspace() else " " for c in project_name)
                        name1 = " ".join(name.split())  # This removes any extra spaces
                        project_description = item.get('description', 'N/A')
                        country_name = item.get('country', 'N/A')
                        country_code = item.get('country_code', 'N/A')
                        standard_version = item.get('gsf_standards_version', 'N/A')
                        project_developer = item.get('project_developer', 'N/A')
                        project_scale = item.get('size', 'N/A')
                        project_type = item.get('type', 'N/A')

                        crediting_period_start = item.get('crediting_period_start_date', 'N/A')
                        crediting_period_end = item.get('crediting_period_end_date', 'N/A')
                        credeting_period = f"{crediting_period_start} - {crediting_period_end}"

                        status = item.get('status', 'N/A')
                        methodology = item.get('methodology', 'N/A')
                        annual_estimated_credits = item.get('estimated_annual_credits', 'N/A')
                        gs_id = item.get('poa_project_sustaincert_id', 'N/A')
                        poa_gs_id = item.get('poa_project_id', 'N/A')
                        document_url = item.get('sustaincert_url', 'N/A')
                        print("URL:::", Url)  # Print URL if data exists
                        # Print the details
                        print(f"Project Name: {name1}, Country: {country_name}, Crediting Period: {credeting_period}")

                        # Add the project data to the list
                        projects_data.append(
                            {
                                'Project_Name': name1,
                                'Description': project_description,
                                'Country': country_name,
                                'Country_Code': country_code,
                                'Standard_Version': standard_version,
                                'Project_Developer': project_developer,
                                'Scale': project_scale,
                                'Type': project_type,
                                'Crediting_Period': credeting_period,
                                'Status': status,
                                'Methodology': methodology,
                                'Annual_Estimated_Credits': annual_estimated_credits,
                                'GS_ID': gs_id,
                                'POA_GS_ID': poa_gs_id,
                                'Document_URL': document_url
                            }
                        )
                else:
                    break  # Exit if there's no data

                break  # Exit retry loop if successful

            except json.JSONDecodeError:
                print(f"JSON decode error for URL: {Url}")
                break  # Exit on decode error

            except requests.exceptions.HTTPError as err:
                print(f"HTTP error: {err}")
                if attempt < retries - 1:  # If not last attempt, retry
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    break  # Exit after max retries
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                break  # Exit on request error

        time.sleep(2)  # Add a delay between requests to avoid hitting rate limits

    # Appending scrapped data to dataframe
    df = pd.DataFrame(projects_data)
    df.to_csv('Standardprojects_productsData.csv', index=False)
    print("Data has been written to 'Standardprojects_productsData.csv'")
    cursor=dbconnection()
    #Insert data into database
    data = pd.read_csv('Standardprojects_productsData.csv')
    cursor.execute("CREATE TABLE projectdetails ('Project_Name nvarchar(50)','Description nvarchar(50)','Country nvarchar(50)','Country_Code nvarchar(50)','Standard_Version nvarchar(50)','Project_Developer nvarchar(50)','Scale nvarchar(50)','Type nvarchar(50)','Crediting_Period nvarchar(50)','Status nvarchar(50)','Methodology nvarchar(50)','Annual_Estimated_Credits int','GS_ID int','POA_GS_ID int','Document_URL nvarchar(50)')")
    for row in df.itertuples():
        cursor.execute('''
        INSERT INTO projects.dbo.projectdetails (Project_Name, Description, Country, Country_Code, Standard_Version, Project_Developer, Scale, Type, Crediting_Period,Status, Methodology,  Annual_Estimated_Credits, GS_ID, POA_GS_ID, Document_URL)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''' , row.name1, row.project_description, row.country_name, row.country_code, row.standard_version, row.project_developer, row.project_scale, row.project_type, row.credeting_period, row.status, row.methodology, row.annual_estimated_credits, row.gs_id, row.poa_gs_id, row.document_url)
        conn.commit()

def main():
    scraper()

if __name__ == "__main__":
    main()