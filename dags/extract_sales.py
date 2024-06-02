import psycopg2
from connect_pg import host, user, db_name, password, port
import requests
from datetime import datetime

date_from = "2023-01-01"
token_seller_1 = "you need write your Token"
token_seller_2 = "you need write your Token"


# test
def main():
    # connect to DB
    connection = psycopg2.connect(
        host=host, user=user, password=password, port=port, database=db_name
    )

    # create Cursor
    cursor = connection.cursor()

    # if table not exist we should create it
    cursor.execute(
        """create table if not exists sales (
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
                    brand VARCHAR(50),
                    owner VARCHAR(20),
					ctl_datetime TIMESTAMP WITHOUT TIME ZONE
                )"""
    )

    cursor.execute("truncate table sales")

    # first load seller_1 data
    response_order_seller_1 = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
        headers={"Authorization": token_seller_1},
        params={"dateFrom": date_from},
    )

    data_order_seller_1 = response_order_seller_1.json()

    for item in data_order_seller_1:
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
            owner = "seller_1"
            ctl_datetime = datetime.now()

            cursor.execute(
                "INSERT INTO sales (srid, gNumber, date_order, lastchangedate, \
                                    priceWithDisc,discountPercent, supplierArticle, isCancel, \
                                    cancelDate, subject, category, brand, owner, ctl_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
                    owner,
                    ctl_datetime,
                ),
            )

    # second load seller_2 data
    response_order_seller_2 = requests.get(
        "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
        headers={"Authorization": token_seller_2},
        params={"dateFrom": date_from},
    )

    data_order_seller_2 = response_order_seller_2.json()

    for item in data_order_seller_2:
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
            owner = "seller_2"
            ctl_datetime = datetime.now()

            cursor.execute(
                "INSERT INTO sales (srid, gNumber, date_order, lastchangedate, \
                                    priceWithDisc,discountPercent, supplierArticle, isCancel, \
                                    cancelDate, subject, category, brand, owner, ctl_datetime) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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
                    owner,
                    ctl_datetime,
                ),
            )

    connection.commit()

    cursor.close()
    connection.close()
    print("closed")


if __name__ == "__main__":
    main()
