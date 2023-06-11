import psycopg2
from prettytable import PrettyTable


class DBManager:
    """Класс для подключения к БД Postgres"""

    def __init__(self, params, database='hh_ru'):
        params['database'] = database
        self.conn = psycopg2.connect(**params)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def get_companies_and_vacancies_count(self):
        """Метод получает список всех компаний и количество вакансий у каждой компании."""
        self.cur.execute("""
                SELECT c.name, c.area, COUNT(v.*) 
                FROM companies c
                LEFT JOIN vacancies v USING(company_id)
                GROUP BY c.name, c.area
                ORDER BY COUNT(v.*) DESC
            """)
        rows = self.cur.fetchall()
        table = PrettyTable()
        table.field_names = ["Компания", "Город или область", "Количество вакансий"]
        for row in rows:
            table.add_row([row[0], row[1], row[2]])
        print(table)

    def get_all_vacancies(self):
        """Метод получает список всех вакансий с указанием названия компании, города,
        названия вакансии, зарплаты и ссылки на вакансию."""
        self.cur.execute("""
                SELECT c.name, c.area, v.name, v.salary_from, v.salary_to, v.salary_currency, v.url
                FROM companies c
                JOIN vacancies v USING(company_id)
                ORDER BY c.name, c.area
            """)
        rows = self.cur.fetchall()
        table = PrettyTable()
        table.field_names = ["Компания", "Город", "Вакансия", "ЗП min", "ЗП max", "Валюта ЗП",
                             "Ссылка на вакансию"]
        for row in rows:
            table.add_row([row[0], row[1], row[2], row[3], row[4], row[5], row[6]])
        print(table)

    def get_avg_salary(self, print_result=True):
        """Метод получает среднюю зарплату по каждой компании в базе данных."""
        query = """
            SELECT c.name, COALESCE(AVG(CASE
                WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL THEN (v.salary_from + v.salary_to) / 2
                WHEN v.salary_from IS NOT NULL AND v.salary_to IS NULL THEN v.salary_from
                WHEN v.salary_from IS NULL AND v.salary_to IS NOT NULL THEN v.salary_to
                ELSE NULL END), 0) AS salary,
                COUNT(CASE WHEN v.salary_from IS NOT NULL OR v.salary_to IS NOT NULL THEN 1 END) AS vacancies_count
            FROM vacancies v
            JOIN companies c USING(company_id)
            WHERE v.salary_from IS NOT NULL OR v.salary_to IS NOT NULL
            GROUP BY c.name
        """
        self.cur.execute(query)
        rows = self.cur.fetchall()

        if print_result:
            table = PrettyTable()
            table.field_names = ["Компания", "Средняя зарплата", "Количество вакансий с указанной ЗП"]
            for row in rows:
                table.add_row([row[0], round(row[1]), row[2]])
            print(table)

        return rows

    def get_vacancies_with_higher_salary(self):
        """Метод получает список вакансий, у которых зарплата выше средней по всем вакансиям."""
        avg_salaries = self.get_avg_salary(print_result=False)
        if avg_salaries is None:
            print("Вакансий не найдено")
            return

        table = PrettyTable()
        table.field_names = ["Компания", "Вакансия", "Указанная ЗП"]

        for company, avg_salary, _ in avg_salaries:
            query = """
                SELECT v.name, COALESCE(AVG(CASE
                    WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL THEN (v.salary_from + v.salary_to) / 2
                    WHEN v.salary_from IS NOT NULL AND v.salary_to IS NULL THEN v.salary_from
                    WHEN v.salary_from IS NULL AND v.salary_to IS NOT NULL THEN v.salary_to
                    ELSE NULL END), 0) AS salary
                FROM vacancies v
                JOIN companies c USING(company_id)
                WHERE c.name = %s
                AND ((v.salary_from > %s OR v.salary_to > %s)
                OR (v.salary_from IS NULL AND v.salary_to > %s)
                OR (v.salary_to IS NULL AND v.salary_from > %s))
                GROUP BY v.name
            """
            args = (company, avg_salary, avg_salary, avg_salary, avg_salary)
            self.cur.execute(query, args)
            rows = self.cur.fetchall()
            if rows:
                for row in rows:
                    if row[1] >= avg_salary:
                        table.add_row([company, row[0], int(row[1])])
        print(table)

    def get_vacancies_with_keyword(self, keyword):
        """Метод получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        self.cur.execute(f"""
                SELECT c.name, v.name, c.area, v.salary_from, v.salary_to, v.salary_currency, v.url
                FROM companies c
                JOIN vacancies v USING(company_id)
                WHERE LOWER(v.name) LIKE LOWER('%%{keyword}%%')
                ORDER BY c.name, v.name
            """)
        rows = self.cur.fetchall()
        if self.cur.rowcount == 0:
            print('Вакансий по заданному ключевому слову не найдено')
        else:
            table = PrettyTable()
            table.field_names = ["Компания", "Вакансия", "Город", "ЗП min", "ЗП max", "Валюта ЗП", "Ссылка на вакансию"]
            for row in rows:
                table.add_row([row[0], row[1], row[2], row[3], row[4], row[5], row[6]])
            print(table)
