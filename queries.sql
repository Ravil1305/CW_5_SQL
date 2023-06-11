--Создание базы данных и таблиц для сохранения данных о компаниях и вакансиях
CREATE DATABASE hh_ru;

CREATE TABLE companies (
                company_id INT PRIMARY KEY,
                name VARCHAR(30) NOT NULL,
                area VARCHAR(50)
            );

CREATE TABLE vacancies (
                name VARCHAR,
                url TEXT,
                salary_from INTEGER,
                salary_to INTEGER,
                salary_currency VARCHAR(3),
                company_id INT REFERENCES companies(company_id)
            );

--Сохранение данных о компаниях и вакансиях
INSERT INTO companies (company_id, name, area)
                VALUES (%s, %s, %s)
                ON CONFLICT (company_id) DO NOTHING;

SELECT company_id FROM companies WHERE company_id = %s;

INSERT INTO vacancies (name, url, salary_from, salary_to, salary_currency, company_id)
                    VALUES (%s, %s, %s, %s, %s, %s);

--Метод получает список всех компаний и количество вакансий у каждой компании.
SELECT c.name, c.area, COUNT(v.*)
                FROM companies c
                LEFT JOIN vacancies v USING(company_id)
                GROUP BY c.name, c.area
                ORDER BY COUNT(v.*) DESC;

--Метод получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
SELECT c.name, c.area, v.name, v.salary_from, v.salary_to, v.salary_currency, v.url
                FROM companies c
                JOIN vacancies v USING(company_id)
                ORDER BY c.name, c.area;

--Метод получает среднюю зарплату по вакансиям
SELECT c.name, COALESCE(AVG(CASE
                WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL THEN (v.salary_from + v.salary_to) / 2
                WHEN v.salary_from IS NOT NULL AND v.salary_to IS NULL THEN v.salary_from
                WHEN v.salary_from IS NULL AND v.salary_to IS NOT NULL THEN v.salary_to
                ELSE NULL END), 0) AS salary,
                COUNT(CASE WHEN v.salary_from IS NOT NULL OR v.salary_to IS NOT NULL THEN 1 END) AS vacancies_count
            FROM vacancies v
            JOIN companies c USING(company_id)
            WHERE v.salary_from IS NOT NULL OR v.salary_to IS NOT NULL
            GROUP BY c.name;

--Метод получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
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
                GROUP BY v.name;

--Метод получает список всех вакансий, в названии которых содержатся переданные в метод слова
SELECT c.name, v.name, c.area, v.salary_from, v.salary_to, v.salary_currency, v.url
                FROM companies c
                JOIN vacancies v USING(company_id)
                WHERE LOWER(v.name) LIKE LOWER('%%{keyword}%%')
                ORDER BY c.name, v.name