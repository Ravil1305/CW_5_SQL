--Создание базы данных и таблиц для сохранения данных о компаниях и вакансиях
CREATE DATABASE hh_ru

CREATE TABLE companies (
                company_id INT PRIMARY KEY,
                name VARCHAR(30) NOT NULL,
                description TEXT,
                area VARCHAR(20)
            )

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

--Сохранение данных о компаниях и вакансиях
INSERT INTO companies (company_id, name, description, area)
                VALUES (%s, %s, %s, %s);
SELECT company_id FROM companies WHERE name = %s;
INSERT INTO vacancies (name, url, salary_from, salary_to, salary_currency, responsibility,
                    published_at, company_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

--Метод получает список всех компаний и количество вакансий у каждой компании.
SELECT c.name, COUNT(v.*) FROM companies c
LEFT JOIN vacancies v ON c.company_id = v.company_id
GROUP BY c.name
ORDER BY COUNT(v.*) DESC

--Метод получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
SELECT c.name, v.name, v.salary_from, v.salary_to, v.salary_currency, v.url
FROM companies c
JOIN vacancies v ON c.company_id = v.company_id
ORDER BY c.name, v.name

--Метод получает среднюю зарплату по вакансиям
SELECT AVG(salary_from + salary_to) / 2 FROM vacancies

--Метод получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
SELECT c.name, v.name, v.salary_from, v.salary_to, v.salary_currency, v.url
FROM companies c
JOIN vacancies v ON c.company_id = v.company_id
WHERE (salary_from + salary_to) / 2 > {avg_salary}
ORDER BY (salary_from + salary_to) / 2 DESC

--Метод получает список всех вакансий, в названии которых содержатся переданные в метод слова
SELECT c.name, v.name, v.salary_from, v.salary_to, v.salary_currency, v.url
FROM companies c
JOIN vacancies v ON c.company_id = v.company_id
WHERE LOWER(v.name) LIKE LOWER('%%{keyword}%%')
ORDER BY c.name, v.name