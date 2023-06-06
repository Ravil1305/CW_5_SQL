import requests


def get_employer_ids(names):
    """Функция для получения списка ID компании на HH.ru по названиям компаний"""
    employer_ids = []
    url = 'https://api.hh.ru/employers'
    for name in names:
        params = {
            'text': name,
            'area': 113,
            'per_page': 100
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            companies = response.json()['items']
            for company in companies:
                if name.lower() == company['name'].lower():
                    company_id = company['id']
                    employer_ids.append(company_id)
        else:
            return None
    return employer_ids
