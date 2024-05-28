import psycopg2
from connect_pg import host, user, db_name, password, port


def main():
    # connect
    connection = psycopg2.connect(
        host=host, user=user, password=password, port=port, database=db_name
    )

    # cursor
    cursor = connection.cursor()

    cursor.execute("""truncate table aggr_ord_sale""")

    cursor.execute(
        """ insert into aggr_ord_sale
            select
                date(date_order) as date_order,
                sum(case when type='orders' then pricewithdisc else 0 end) as ord_sum,
                sum(case when type='sales' then pricewithdisc else 0 end) as sale_sum,
                sum(case when type='orders' then 1 else 0 end) as ord_cnt,
                sum(case when type='orders' then 1 else 0 end) as ord_cnt
            from
                (select
                    *,
                    'sales' as type
                from sales
                union
                select
                    *,
                    'orders' as type
                from orders) t
            where date(date_order) >= '2024-05-01'
            group by date(date_order)
            order by date(date_order)"""
    )

    connection.commit()

    cursor.close()
    connection.close()
    print("closed")


if __name__ == "__main__":
    main()
