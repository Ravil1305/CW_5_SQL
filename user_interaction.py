from DBManager_class import DBManager
from config import config
from employer_ids import get_employer_ids
from utils import get_hh_data, create_database, save_data_to_database

params = config()


def user_interaction():
    """Функция создания базы данных и заполнения таблиц по введенным пользователем названиям интересующих компаний,
    работы с методами класса DBManager"""
    while True:
        company_names = input("Введите названия интересующих компаний через запятую: ")
        company_names_list = [name.strip() for name in company_names.split(",")]
        company_names_user = [name for name in company_names_list if name]

        if not company_names_user:
            print("Вы не ввели названий компаний")
            choice = input("Нажмите '1', чтобы начать заново, или '2', чтобы выйти из программы: ")
            if choice == "1":
                continue
            elif choice == "2":
                print("До свидания!")
                return
        else:
            print(f"Ок! Вы ввели названия компаний: {', '.join(company_names_user)}")
            break

    company_ids = get_employer_ids(company_names_user)
    data = get_hh_data(company_ids)
    create_database('hh_ru', params)
    save_data_to_database(data, 'hh_ru', params)

    run_user_dbm = True
    db = DBManager(params)
    while run_user_dbm:
        print("Выберите действие:")
        print("1 - получить список всех компаний и количество вакансий у каждой компании")
        print("2 - получить список всех вакансий с указанием названия компании, города, "
              "названия вакансии, зарплаты и ссылки на вакансию")
        print("3 - получить среднюю зарплату по вакансиям каждой компании в базе данных")
        print("4 - получить список вакансий, у которых зарплата выше средней по всем вакансиям компании")
        print("5 - поиск вакансий по ключевому слову")
        print("6 - выход")
        choice = input("Введите номер действия: ")
        if choice == "1":
            db.get_companies_and_vacancies_count()
        elif choice == "2":
            db.get_all_vacancies()
        elif choice == "3":
            db.get_avg_salary()
        elif choice == "4":
            db.get_vacancies_with_higher_salary()
        elif choice == "5":
            keyword = input("Введите ключевое слово для поиска: ")
            db.get_vacancies_with_keyword(keyword)
        elif choice == "6":
            print("До свидания!")
            run_user_dbm = False
        else:
            print("Некорректный ввод")


def main():
    user_interaction()
