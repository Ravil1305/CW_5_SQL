from config import config
from employer_ids import get_employer_ids
from utils import get_hh_data, create_database, save_data_to_database


def main():
    company_names = ['ЕВРАЗ', 'ОАО Металлист', 'УГМК', 'Яндекс', 'Газпром',
                     'Тинькофф', 'Мегафон', 'СкайПро', 'VK', 'Сбербанк-Сервис']
    company_ids = get_employer_ids(company_names)
    params = config()
    data = get_hh_data(company_ids)
    create_database('hh_ru', params)
    save_data_to_database(data, 'hh_ru', params)


if __name__ == '__main__':
    main()
