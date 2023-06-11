import requests


def get_employer_ids(names):
    """Функция для получения списка ID компаний на HH.ru по названиям компаний"""
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
            found = False
            for company in companies:
                if name.lower() == company['name'].lower():
                    company_id = company['id']
                    employer_ids.append(company_id)
                    found = True
            if not found:
                print(f"Увы! Компания {name} не найдена на HH.ru")
        else:
            print(f"Ошибка HTTP: {response.status_code}")
    return employer_ids
