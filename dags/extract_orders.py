import psycopg2
from connect_pg import host, user, db_name, password, port
import requests

date_from = "2023-01-01"
token = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNzY5Mzc2OCwiaWQiOiI3MDdhNjFkZC01MWUwLTRjNDktYjNhZC02N2Y0Y2VlYmQyOGQiLCJpaWQiOjUzNzQ1NTQ1LCJvaWQiOjQ0MTgyMCwicyI6MTA3Mzc0MTg1Niwic2lkIjoiMDliZTA5NTUtM2ViZC00MDNkLWJkNzctNGQwYjhmNzBkZjRiIiwidWlkIjo1Mzc0NTU0NX0.niVswDAdI503OlyTG0d6eCYZZP1f0wmGURj_y-PsWP0PdNzvZAqKw_4DpSuvfRqkn0_2EJcMJ2X48yd-0yow3Q"


def main():
    response_order = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
        headers={"Authorization": token},
        params={"dateFrom": date_from},
    )
    data_order = response_order.json()

    # connect
    connection = psycopg2.connect(
        host=host, user=user, password=password, port=port, database=db_name
    )

    # cursor
    cursor = connection.cursor()

    cursor.execute(
        """create table if not exists orders (
                    srid VARCHAR(100),
                    gNumber VARCHAR(25),
                    date_order TIMESTAMP WITHOUT TIME ZONE,
                    lastchangedate TIMESTAMP WITHOUT TIME ZONE,
                    priceWithDisc INT,
                    discountpercent DECIMAL,
                    supplierarticle VARCHAR(50),
                    iscancel BOOL,
                    cancelDate TIMESTAMP WITHOUT TIME ZONE,
                    subject VARCHAR(50),
                    category VARCHAR(50),
                    brand VARCHAR(50)
                )"""
    )

    cursor.execute("truncate table orders")

    for item in data_order:
        if isinstance(item, dict):
            srid = item.get("srid")
            gNumber = item.get("gNumber")
            date_order = item.get("date")
            lastchangedate = item.get("lastChangeDate")
            priceWithDisc = item.get("priceWithDisc")
            discountPercent = item.get("discountPercent")
            supplierArticle = item.get("supplierArticle")
            isCancel = item.get("isCancel")
            cancelDate = item.get("cancelDate")
            subject = item.get("subject")
            category = item.get("category")
            brand = item.get("brand")

            cursor.execute(
                "INSERT INTO orders (srid, gNumber, date_order, lastchangedate, \
                                    priceWithDisc,discountPercent, supplierArticle, isCancel, \
                                    cancelDate, subject, category, brand) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    srid,
                    gNumber,
                    date_order,
                    lastchangedate,
                    priceWithDisc,
                    discountPercent,
                    supplierArticle,
                    isCancel,
                    cancelDate,
                    subject,
                    category,
                    brand,
                ),
            )

    connection.commit()

    cursor.close()
    connection.close()
    print("closed")


if __name__ == "__main__":
    main()
