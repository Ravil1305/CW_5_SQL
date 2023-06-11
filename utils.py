import psycopg2
import requests


def get_hh_data(company_ids):
    """Получение данных о компаниях и вакансиях"""
    hh_data = []

    for company_id in company_ids:
        company_url = f'https://api.hh.ru/employers/{company_id}'
        company_response = requests.get(company_url)
        company_data = company_response.json()

        company_info = {
            'id': company_data['id'],
            'name': company_data['name'],
            'area': company_data['area']['name']
        }

        vacancies_url = f'https://api.hh.ru/vacancies'
        params = {
            'employer_id': company_id,
            'per_page': 50,
            'page': 0,
            'area': 113
        }

        vacancies_response = requests.get(vacancies_url, params=params)
        vacancies_data = vacancies_response.json()
        vacancies = []

        for page in range(0, vacancies_data['pages']):
            params['page'] = page
            vacancies_response = requests.get(vacancies_url, params=params)
            vacancies_data = vacancies_response.json()

            for vacancy in vacancies_data['items']:
                vacancy_info = {
                    'name': vacancy['name'],
                    'url': vacancy['alternate_url'],
                    'salary_from': vacancy['salary']['from'] if vacancy['salary'] else None,
                    'salary_to': vacancy['salary']['to'] if vacancy['salary'] else None,
                    'salary_currency': vacancy['salary']['currency'] if vacancy['salary'] else None,
                    'company_id': vacancy['employer']['id'] if vacancy['employer'] else None
                }
                vacancies.append(vacancy_info)

        hh_data.append({'company': company_info, 'vacancies': vacancies})

    return hh_data


def create_database(database_name, params):
    """Создание базы данных и таблиц для сохранения данных о компаниях и вакансиях"""

    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE companies (
                company_id INT PRIMARY KEY,
                name VARCHAR(30) NOT NULL,
                area VARCHAR(50)
            )
        """)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacancies (
                name VARCHAR,
                url TEXT,
                salary_from INTEGER,
                salary_to INTEGER,
                salary_currency VARCHAR(3),
                company_id INT REFERENCES companies(company_id)
            )
        """)

    conn.commit()
    conn.close()


def save_data_to_database(data, database_name, params):
    """Сохранение данных о компаниях и вакансиях"""
    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for company_data in data:
            company = company_data['company']
            cur.execute("""
                INSERT INTO companies (company_id, name, area)
                VALUES (%s, %s, %s)
                ON CONFLICT (company_id) DO NOTHING
            """, (company['id'], company['name'], company['area']))

            cur.execute("""
                SELECT company_id FROM companies WHERE company_id = %s
            """, (company['id'],))
            company_id = cur.fetchone()[0]

            for vacancy in company_data['vacancies']:
                cur.execute("""
                    INSERT INTO vacancies (name, url, salary_from, salary_to, salary_currency, company_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (vacancy['name'], vacancy['url'], vacancy['salary_from'], vacancy['salary_to'],
                      vacancy['salary_currency'], company_id))

        conn.commit()
        conn.close()
