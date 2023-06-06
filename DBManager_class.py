import psycopg2

from config import config


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
            SELECT c.name, COUNT(v.*) FROM companies c
            LEFT JOIN vacancies v ON c.company_id = v.company_id
            GROUP BY c.name
            ORDER BY COUNT(v.*) DESC
        """)
        return self.cur.fetchall()

    def get_all_vacancies(self):
        """Метод получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        self.cur.execute("""
            SELECT c.name, v.name, v.salary_from, v.salary_to, v.salary_currency, v.url
            FROM companies c
            JOIN vacancies v ON c.company_id = v.company_id
            ORDER BY c.name, v.name
        """)
        return self.cur.fetchall()

    def get_avg_salary(self):
        """Метод получает среднюю зарплату по вакансиям."""
        self.cur.execute("""
            SELECT AVG(salary_from + salary_to) / 2 FROM vacancies
        """)
        result = self.cur.fetchone()
        self.conn.commit()
        return result[0] if result else None

    def get_vacancies_with_higher_salary(self):
        """Метод получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        avg_salary = self.get_avg_salary()
        if avg_salary is None:
            return []
        self.cur.execute(f"""
            SELECT c.name, v.name, v.salary_from, v.salary_to, v.salary_currency, v.url
            FROM companies c
            JOIN vacancies v ON c.company_id = v.company_id
            WHERE (salary_from + salary_to) / 2 > {avg_salary}
            ORDER BY (salary_from + salary_to) / 2 DESC
        """)
        return self.cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """Метод получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        self.cur.execute(f"""
            SELECT c.name, v.name, v.salary_from, v.salary_to, v.salary_currency, v.url
            FROM companies c
            JOIN vacancies v ON c.company_id = v.company_id
            WHERE LOWER(v.name) LIKE LOWER('%%{keyword}%%')
            ORDER BY c.name, v.name
        """)
        return self.cur.fetchall()


params = config()
db = DBManager(params)

result = db.get_companies_and_vacancies_count()
print(result)

result = db.get_all_vacancies()
print(result)

avg_salary = db.get_avg_salary()
print(avg_salary)

result = db.get_vacancies_with_higher_salary()
print(result)

result = db.get_vacancies_with_keyword('python')
print(result)
