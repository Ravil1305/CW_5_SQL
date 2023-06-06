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
            'description': company_data['description'],
            'area': company_data['area']['name']
        }

        vacancies_url = f'https://api.hh.ru/vacancies?employer_id={company_id}'
        vacancies_response = requests.get(vacancies_url)
        vacancies_data = vacancies_response.json()
        vacancies = []

        for vacancy in vacancies_data['items']:
            vacancy_info = {
                'name': vacancy['name'],
                'url': vacancy['alternate_url'],
                'salary_from': vacancy['salary']['from'] if vacancy['salary'] else None,
                'salary_to': vacancy['salary']['to'] if vacancy['salary'] else None,
                'salary_currency': vacancy['salary']['currency'] if vacancy['salary'] else None,
                'responsibility': vacancy['snippet']['responsibility'] if vacancy['snippet'] else None,
                'published_at': vacancy['published_at'],
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

    cur.execute(f"DROP DATABASE {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE companies (
                company_id INT PRIMARY KEY,
                name VARCHAR(30) NOT NULL,
                description TEXT,
                area VARCHAR(20)
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
                responsibility TEXT,
                published_at TIMESTAMP,
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
                INSERT INTO companies (company_id, name, description, area)
                VALUES (%s, %s, %s, %s)
                """,
                        (company['id'], company['name'], company['description'], company['area']))

            cur.execute("""
                    SELECT company_id FROM companies WHERE name = %s
                """, (company['name'],))
            company_id = cur.fetchone()[0]

            for vacancy in company_data['vacancies']:
                cur.execute("""
                    INSERT INTO vacancies (name, url, salary_from, salary_to, salary_currency, responsibility, 
                    published_at, company_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (vacancy['name'], vacancy['url'], vacancy['salary_from'], vacancy['salary_to'],
                      vacancy['salary_currency'], vacancy['responsibility'], vacancy['published_at'], company_id))

        conn.commit()
        conn.close()
